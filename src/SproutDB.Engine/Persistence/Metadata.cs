using Microsoft.Extensions.Options;
using System.Security.Cryptography;
using System.Text;

namespace SproutDB.Engine.Persistence;


/// <summary>
/// This service is created once per database and is responsible for managing the commit persistence.
/// </summary>
public interface ICommitPersistenceService
{
    Task<bool> CreateDatabase(string database);
    Task<string> WriteCommit(string database, string branch, string query, string author);
}
public class CommitPersistenceService(
    IOptions<StorageOptions> storageOptions,
    IDeltaWriterService deltaWriterService,
    IMetadataWriter metadataWriter,
    ICommitIdGenerator commitIdGenerator)
    : ICommitPersistenceService
{
    private string? _lastCommitId;

    public async Task<bool> CreateDatabase(string database)
    {

        var branch = "main"; // default branch

        //prepare storage
        var basePath = storageOptions.Value.BasePath;
        var targetPath = Path.Combine(basePath, database);
        Directory.CreateDirectory(targetPath);
        Directory.CreateDirectory(Path.Combine(targetPath, "delta"));
        Directory.CreateDirectory(Path.Combine(targetPath, "meta"));


        //create .delta file
        deltaWriterService.Initialize(database, branch);
        var deltaResult = await deltaWriterService.CreateDeltaSegment(database);
        if (deltaResult == null)
        {
            return false; // Delta segment already exists
        }

        //creade .meta file
        var metadataSuccess = await metadataWriter.CreateBranch(database, branch, null, null, deltaResult.Value.Name);
        if (!metadataSuccess)
        {
            return false; // Failed to create metadata for the branch
        }

        return true; // Database created successfully

    }

    public async Task<string> WriteCommit(string database, string branch, string query, string author)
    {
        var commitId = commitIdGenerator.GetNextCommitId(database, branch, _lastCommitId, author, query);
        _lastCommitId = commitId;

        var delta = await deltaWriterService.WriteCommit(commitId, query, author, [_lastCommitId]);




        return commitId;
    }
}

public interface ICommitIdGenerator
{
    string GetNextCommitId(string database, string branch, string? parentCommit, string author, string query);
}
public class CommitIdGenerator : ICommitIdGenerator
{
    public string GetNextCommitId(string database, string branch, string? parentCommit, string author, string query)
    {
        //Commit ID = sha256(parent_commits + timestamp + author + query)
        var rawData = $"{parentCommit}_{DateTime.UtcNow.Ticks}_{author}_{query}";
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(rawData));
        //base64 for compactness
        return Convert.ToBase64String(hashBytes);
    }
}

public interface IMetadataWriter
{
    Task<bool> CreateBranch(string database, string branch, string? baseBranch, uint? baseCommit, string currentDelta);
    //Task AppendDelta(string database, string branch, string name);
    //Task AppendCommit(string database, string branch, string commitId, long position);
}
public class MetadataWriter(
    IOptions<StorageOptions> storageOptions)
    : IMetadataWriter
{
    private string? _database;
    private string? _branch;
    public Task<bool> CreateBranch(string database, string branch, string? baseBranch, uint? baseCommit, string currentDelta)
    {
        _database = database;
        _branch = branch;
        EnsureInitialized();

        var fullName = $"branch_{branch}.meta";
        var basePath = storageOptions.Value.BasePath;
        var metaPath = Path.Combine(basePath, _database, "meta", fullName);
        if (!File.Exists(basePath))
        {
            using var fileStream = File.CreateText(metaPath);
            fileStream.WriteLine($"Branch: {branch}");
            if (baseBranch != null)
            {
                fileStream.WriteLine($"Base Branch: {baseBranch}");
            }
            if (baseCommit.HasValue)
            {
                fileStream.WriteLine($"Base Commit: {baseCommit.Value}");
            }
            fileStream.WriteLine("Files:");
            fileStream.WriteLine($"  {currentDelta}: ");
            fileStream.Flush();
            fileStream.Close();
            return Task.FromResult(true);
        }
        else
        {
            return Task.FromResult(false);
        }
    }

    private void EnsureInitialized()
    {
        if (_database == null || _branch == null)
        {
            throw new InvalidOperationException("DeltaWriterService must be initialized with database and branch before use.");
        }
        var targetPath = Path.Combine(storageOptions.Value.BasePath, _database);
        Directory.CreateDirectory(targetPath);
        Directory.CreateDirectory(Path.Combine(targetPath, "meta"));
    }
}
public interface IDeltaWriterService
{
    void Initialize(string database, string branch);
    Task<DeltaData?> CreateDeltaSegment(string branch);
    Task<DeltaEntry?> WriteCommit(string commitId, string query, string author, string[] parentBranches);
}
public class DeltaWriterService(IOptions<StorageOptions> storageOptions) : IDeltaWriterService
{
    private string? _database;
    private string? _branch;
    private int _sequence;

    public void Initialize(string database, string branch)
    {
        _database = database;
        _branch = branch;
        _sequence = 1;
    }
    public Task<DeltaData?> CreateDeltaSegment(string branch)
    {
        EnsureInitialized();
        var fullName = $"segment_{branch}_{_sequence}";
        var basePath = storageOptions.Value.BasePath;
        var deltaPath = Path.Combine(basePath, _database!, "delta", $"{fullName}.delta");
        if (!File.Exists(deltaPath))
        {
            using var fileStream = File.CreateText(deltaPath);
            fileStream.WriteLine($"Segment: {fullName}");
            fileStream.WriteLine($"Branch: {branch}");
            fileStream.WriteLine();
            fileStream.Flush();
            fileStream.Close();
        }
        else
        {
            return Task.FromResult<DeltaData?>(null); // Delta file already exists
        }
        return Task.FromResult<DeltaData?>(new DeltaData($"{fullName}.delta", _sequence));
    }
    public Task<DeltaEntry?> WriteCommit(string commitId, string query, string author, string[] parentBranches)
    {
        EnsureInitialized();
        var fullName = $"segment_{_branch}_{_sequence}";
        var basePath = storageOptions.Value.BasePath;
        var deltaPath = Path.Combine(basePath, _database!, "delta", $"{fullName}.delta");
        if (!File.Exists(deltaPath))
        {
            return Task.FromResult<DeltaEntry?>(null); // Delta file does not exist
        }
        long position;
        using (var fileStream = new FileStream(deltaPath, FileMode.Append, FileAccess.Write, FileShare.None))
        using (var writer = new StreamWriter(fileStream))
        {
            position = fileStream.Position;
            writer.WriteLine($"{commitId}:");
            if(parentBranches.Length > 0)
            {
                writer.WriteLine($"  Parents: [{string.Join(", ", parentBranches)}]");
            }
            writer.WriteLine($"  Timestamp: {DateTime.UtcNow:O}");
            writer.WriteLine($"  Author: {author}");
            writer.WriteLine($"  Query: {query.Replace('\r', ' ').Replace('\n', ' ')}");
            writer.WriteLine();
            writer.Flush();
        }
        return Task.FromResult<DeltaEntry?>(new DeltaEntry(commitId, position));
    }


    private void EnsureInitialized()
    {
        if (_database == null || _branch == null)
        {
            throw new InvalidOperationException("DeltaWriterService must be initialized with database and branch before use.");
        }
        var targetPath = Path.Combine(storageOptions.Value.BasePath, _database);
        Directory.CreateDirectory(targetPath);
        Directory.CreateDirectory(Path.Combine(targetPath, "delta"));
    }
}
public record struct DeltaData(string Name, int _sequence);
public record struct DeltaEntry(string CommitId, long Position);

public class StorageOptions
{
    public string BasePath { get; set; } = string.Empty;
}

using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SproutDB.Engine.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SproutDB.Engine.Tests.ICommitPersistenceServiceTests;

[TestClass]
public class CommitPersistenceServiceTests
{

    [TestCleanup]
    public void Cleanup()
    {
        // Clean up any files created during the tests
        var basePath = "C:\\SproutDB\\UnitTestCommit";
        if (Directory.Exists(basePath))
        {
            Directory.Delete(basePath, true);
        }
    }

    [TestMethod]
    public async Task TestCreateDatabase_Works()
    {
        //arrange
        var options = Options.Create(new StorageOptions
        {
            BasePath = "C:\\SproutDB\\UnitTestCommit"
        });
        var deltaWriterService = new DeltaWriterService(options);
        var metadataWriter = new MetadataWriter(options);
        var sut = new CommitPersistenceService(options, deltaWriterService, metadataWriter);

        //act
        var result = await sut.CreateDatabase("testDatabase");

        //assert
        Assert.IsTrue(result, "Database creation should succeed.");
        var deltaFileExists = File.Exists(Path.Combine(options.Value.BasePath, "testDatabase\\delta\\segment_testDatabase_1.delta"));
        Assert.IsTrue(deltaFileExists, "Delta file should be created.");
        var deltaFileContent = await File.ReadAllTextAsync(Path.Combine(options.Value.BasePath, "testDatabase\\delta\\segment_testDatabase_1.delta"));
        Assert.IsTrue(deltaFileContent.Contains("segment_testDatabase_1"), "Delta file should contain the segment name.");

        var metaFileExists = File.Exists(Path.Combine(options.Value.BasePath, "testDatabase\\meta\\branch_main.meta"));
        Assert.IsTrue(metaFileExists, "Metadata file should be created.");
        var metaFileContent = await File.ReadAllTextAsync(Path.Combine(options.Value.BasePath, "testDatabase\\meta\\branch_main.meta"));
        Assert.IsTrue(metaFileContent.Contains("Branch: main"), "Metadata file should contain the branch name.");
        Assert.IsTrue(metaFileContent.Contains("segment_testDatabase_1"), "Metadata file should contain the delta segment name.");
    }


    [TestMethod]
    public async Task TestCreateCommit_Workd()
    {
        //arrange
        var options = Options.Create(new StorageOptions
        {
            BasePath = "C:\\SproutDB\\UnitTestCommit"
        });
        var deltaWriterService = new DeltaWriterService(options);
        var metadataWriter = new MetadataWriter(options);
        var sut = new CommitPersistenceService(options, deltaWriterService, metadataWriter);

        //act
        await sut.CreateDatabase("testDatabase");
        await sut.WriteCommit("testDatabase", "main", "create table users");

    }
}


[TestClass]
public class DeltaWriterServiceTests
{
    [TestCleanup]
    public void Cleanup()
    {
        // Clean up any files created during the tests
        var basePath = "C:\\SproutDB\\UnitTestDelta";
        if (Directory.Exists(basePath))
        {
            Directory.Delete(basePath, true);
        }
    }

    [TestMethod]
    public async Task TestCreateDeltaSegment_Works()
    {
        //arrange
        var options = Options.Create(new StorageOptions
        {
            BasePath = "C:\\SproutDB\\UnitTestDelta"
        });
        var sut = new DeltaWriterService(options);

        //act
        sut.Initialize("testDatabase", "main");
        var result = await sut.CreateDeltaSegment("testDatabase");

        //assert
        Assert.IsNotNull(result, "Delta segment creation should succeed.");
        var deltaFileExists = File.Exists(Path.Combine(options.Value.BasePath, "testDatabase\\delta\\segment_testDatabase_1.delta"));
        Assert.IsTrue(deltaFileExists, "Delta file should be created.");
        var deltaFileContent = await File.ReadAllTextAsync(Path.Combine(options.Value.BasePath, "testDatabase\\delta\\segment_testDatabase_1.delta"));
        Assert.IsTrue(deltaFileContent.Contains("segment_testDatabase_1"), "Delta file should contain the segment name.");
        Assert.IsTrue(deltaFileContent.Contains("testDatabase"), "Delta file should contain the branch name.");
        Assert.AreEqual("segment_testDatabase_1.delta", result.Value.Name, "Delta segment name should match.");
    }
}

[TestClass]
public class MetadataWriterTests
{
    [TestCleanup]
    public void Cleanup()
    {
        // Clean up any files created during the tests
        var basePath = "C:\\SproutDB\\UnitTestMeta";
        if (Directory.Exists(basePath))
        {
            Directory.Delete(basePath, true);
        }
    }

    [TestMethod]
    public async Task TestCreateBranch_Works()
    {
        //arrange
        var options = Options.Create(new StorageOptions
        {
            BasePath = "C:\\SproutDB\\UnitTestMeta"
        });
        var sut = new MetadataWriter(options);

        //act
        var result = await sut.CreateBranch("testDatabase", "main", "bb1", 4201337, "initialDelta");

        //assert
        Assert.IsTrue(result, "Database creation should succeed.");
        var metaFileExists = File.Exists(Path.Combine(options.Value.BasePath, "testDatabase\\meta\\branch_main.meta"));
        Assert.IsTrue(metaFileExists, "Metadata file should be created.");
        var metaFileContent = await File.ReadAllTextAsync(Path.Combine(options.Value.BasePath, "testDatabase\\meta\\branch_main.meta"));
        Assert.IsTrue(metaFileContent.Contains("Branch: main"), "Metadata file should contain the branch name.");
        Assert.IsTrue(metaFileContent.Contains("Base Branch: bb1"), "Metadata file should contain the base branch name.");
        Assert.IsTrue(metaFileContent.Contains("Base Commit: 4201337"), "Metadata file should contain the base commit number.");
        Assert.IsTrue(metaFileContent.Contains("initialDelta"), "Metadata file should contain the delta segment name.");
    }

}
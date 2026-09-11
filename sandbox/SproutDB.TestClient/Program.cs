// End-to-end test client for a SproutDB server reached over HTTP (e.g. the sandbox):
//
//   dotnet run --project sandbox/SproutDB.TestClient -- <scenario> [--url=http://localhost:5199/] [--key=sdb_ak_1234]
//
//   features   remote API: CAS under concurrency, parameters, typed API, async, OnChange, errors
//   stream     writes until the server goes away; every acknowledged write goes to --acked=<file>
//   verify     after a server restart: every acknowledged write is there, exactly once

using Microsoft.Extensions.DependencyInjection;
using SproutDB.Core;
using SproutDB.Core.DependencyInjection;

var scenario = args.FirstOrDefault(a => !a.StartsWith("--")) ?? "features";
var url = Arg("url", "http://localhost:5199/");
var key = Arg("key", "sdb_ak_1234");
var ackedFile = Arg("acked", Path.Combine(Path.GetTempPath(), "sproutdb-e2e-acked.txt"));

// Registered exactly like an embedded engine would be — only AddSproutDBClient instead of AddSproutDB
var services = new ServiceCollection();
services.AddSproutDBClient(o =>
{
    o.BaseAddress = new Uri(url);
    o.ApiKey = key;
});
using var provider = services.BuildServiceProvider();
var server = provider.GetRequiredService<ISproutServer>();

var failures = 0;
void Check(bool ok, string name, string detail = "")
{
    if (!ok) failures++;
    Console.WriteLine($"{(ok ? "PASS" : "FAIL")}  {name}{(detail.Length > 0 ? $"  ({detail})" : "")}");
}

switch (scenario)
{
    case "features": await Features(); break;
    case "stream": await Stream(); break;
    case "verify": Verify(); break;
    default:
        Console.WriteLine($"unknown scenario '{scenario}'");
        return 2;
}

Console.WriteLine(failures == 0 ? "ALL PASSED" : $"{failures} FAILED");
return failures == 0 ? 0 : 1;

// ── Scenarios ─────────────────────────────────────────────────

async Task Features()
{
    var db = server.GetOrCreateDatabase("e2e");
    Reset(db);

    // Parameters: hostile values stay values
    const string hostile = "x'; purge table gs; ## \\";
    db.Query("upsert gs {k: @k, etag: 'E1'}", new { k = hostile });
    var found = db.Query("get gs select k where k = @k", new { k = hostile })[0];
    Check(found.Data?.Count == 1 && found.Data[0]["k"] as string == hostile, "parameters: hostile value round-trips");
    Check(db.Query("describe gs")[0].Errors is null, "parameters: table still exists");

    // CAS: many concurrent writers expecting the same etag — exactly one wins per round
    db.Query("upsert gs {k: 'cas', etag: 'R0'}");
    var roundsOk = 0;
    for (var round = 0; round < 10; round++)
    {
        var expected = $"R{round}";
        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(w =>
            db.QueryAsync("upsert gs {k: 'cas', etag: @next, payload: @p} on k when etag = @expected",
                new { next = $"R{round + 1}", p = $"writer {w}", expected }).AsTask()));
        var winners = results.Count(r => r[0].Errors is null);
        var conflicts = results.Count(r => r[0].Errors?[0].Code == "CONDITION_FAILED" && r[0].Data?.Count == 1);
        if (winners == 1 && conflicts == 15) roundsOk++;
    }
    Check(roundsOk == 10, "CAS: 16 concurrent writers x 10 rounds, exactly one winner each", $"{roundsOk}/10");

    var stale = db.Query("upsert gs {k: 'cas', etag: 'X'} on k when etag = 'R0'")[0];
    Check(stale.Errors?[0].Code == "CONDITION_FAILED" && stale.Data?[0]["etag"] as string == "R10",
        "CAS: conflict reports the stored etag");

    Check(db.Query("upsert gs {k: 'new', etag: 'N1'} on k when not exists")[0].Errors is null, "when not exists inserts");
    Check(db.Query("upsert gs {k: 'new', etag: 'N2'} on k when not exists")[0].Errors?[0].Code == "CONDITION_FAILED",
        "when not exists rejects existing row");
    Check(db.Query("upsert gs {k: 'ghost', etag: 'G'} on k when exists")[0].Errors?[0].Code == "CONDITION_FAILED",
        "when exists rejects missing row");
    Check(db.Query("delete gs where k = 'new' and etag = 'WRONG' expect 1")[0].Errors?[0].Code == "EXPECTATION_FAILED",
        "delete expect: mismatch deletes nothing");
    Check(db.Query("delete gs where k = 'new' and etag = 'N1' expect 1")[0].Affected == 1, "delete expect: match deletes");

    // Transaction with a failing condition rolls back
    var tx = db.Query("atomic; upsert gs {k: 'txa', etag: 'A'}; upsert gs {k: 'cas', etag: 'Y'} on k when etag = 'nope'; commit");
    Check(tx.Count == 1 && tx[0].Errors?[0].Code == "CONDITION_FAILED"
          && (db.Query("get gs where k = 'txa'")[0].Data?.Count ?? 0) == 0, "atomic + CONDITION_FAILED rolls back");

    // Typed API over HTTP
    var grains = db.Table<Grain>("gs");
    grains.Upsert(new Grain { K = "typed", Etag = "T1", N = 42 });
    var typed = db.Table<Grain>("gs").FirstOrDefault(g => g.K == "typed");
    Check(typed is { Etag: "T1", N: 42 } && typed.Id > 0, "typed API: upsert + read");
    var typedCas = grains.Upsert(new Grain { K = "typed", Etag = "T2", N = 43 }, on: g => g.K, when: g => g.Etag == "T1");
    Check(typedCas.Errors is null, "typed API: conditional upsert");

    // Error messages
    var misplaced = db.Query("get gs where k = 'a' select k")[0];
    Check(misplaced.Errors?[0].Message.Contains("'select' must come directly after the table name") == true,
        "error: select after where is explained");
    var missing = db.Query("get gs where k = @nope")[0];
    Check(missing.Errors?[0].Code == "PARAMETER_ERROR", "error: missing parameter");
    var ddl = db.Query("atomic; create table x (v sint); commit")[0];
    Check(ddl.Errors?[0].Code == "SYNTAX_ERROR", "error: DDL inside atomic rejected");

    // OnChange over SignalR (WebSockets)
    using var signal = new ManualResetEventSlim();
    SproutResponse? change = null;
    using (db.OnChange("gs", r => { change = r; signal.Set(); }))
    {
        db.Query("upsert gs {k: 'notify', etag: 'N'}");
        Check(signal.Wait(TimeSpan.FromSeconds(10)) && change?.Data?[0]["k"] as string == "notify",
            "OnChange: notification over SignalR");
    }

    await Task.CompletedTask;
}

async Task Stream()
{
    var db = server.GetOrCreateDatabase("e2e");
    Reset(db);
    File.WriteAllText(ackedFile, "");

    using var acked = new StreamWriter(ackedFile, append: true) { AutoFlush = true };
    var i = 0;
    try
    {
        while (true)
        {
            switch (i % 3)
            {
                case 0: // single upsert on a unique key
                    Ensure(await db.QueryAsync("upsert gs {k: @k, n: @n} on k", new { k = $"w{i}", n = i }));
                    acked.WriteLine($"gs w{i}");
                    break;
                case 1: // bulk insert without 'on' (replay must not duplicate these)
                    Ensure(await db.QueryAsync($"upsert log [{{k: 'b{i}-0'}}, {{k: 'b{i}-1'}}, {{k: 'b{i}-2'}}]"));
                    acked.WriteLine($"log b{i}-0");
                    acked.WriteLine($"log b{i}-1");
                    acked.WriteLine($"log b{i}-2");
                    break;
                default: // transaction
                    Ensure(await db.QueryAsync($"atomic; upsert log {{k: 't{i}-a'}}; upsert log {{k: 't{i}-b'}}; commit"));
                    acked.WriteLine($"log t{i}-a");
                    acked.WriteLine($"log t{i}-b");
                    break;
            }
            i++;
        }
    }
    catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException or IOException)
    {
        Console.WriteLine($"server gone after {i} acknowledged write statements ({ex.GetType().Name})");
    }
}

void Verify()
{
    var db = server.SelectDatabase("e2e");
    var expected = File.ReadAllLines(ackedFile).Where(l => l.Length > 0).ToList();

    var missing = 0;
    var duplicated = 0;
    foreach (var line in expected)
    {
        var parts = line.Split(' ');
        var count = db.Query($"get {parts[0]} where k = @k count", new { k = parts[1] })[0].Affected;
        if (count == 0) missing++;
        if (count > 1) duplicated++;
    }

    var gsRows = db.Query("get gs count")[0].Affected;
    var logRows = db.Query("get log count")[0].Affected;
    var expectedGs = expected.Count(l => l.StartsWith("gs "));
    var expectedLog = expected.Count(l => l.StartsWith("log "));

    Check(missing == 0, "every acknowledged write survived the kill", $"{expected.Count} checked, {missing} missing");
    Check(duplicated == 0, "no row was replayed twice", $"{duplicated} duplicated");
    // At most one statement can be in flight when the server dies (written but not acknowledged)
    Check(gsRows - expectedGs is >= 0 and <= 1, "gs row count", $"{gsRows} rows, {expectedGs} acknowledged");
    Check(logRows - expectedLog is >= 0 and <= 3, "log row count", $"{logRows} rows, {expectedLog} acknowledged");
}

// ── Helpers ───────────────────────────────────────────────────

void Reset(ISproutDatabase db)
{
    db.Query("purge table gs");
    db.Query("purge table log");
    Ensure(db.Query("create table gs (k string 64 strict unique, etag string 32, payload string 200, n slong)"));
    Ensure(db.Query("create table log (k string 64)"));
}

static void Ensure(List<SproutResponse> responses)
{
    foreach (var r in responses)
    {
        if (r.Errors is { Count: > 0 } errors)
            throw new InvalidOperationException($"{errors[0].Code}: {errors[0].Message}");
    }
}

string Arg(string name, string fallback)
{
    var prefix = $"--{name}=";
    return args.FirstOrDefault(a => a.StartsWith(prefix))?[prefix.Length..] ?? fallback;
}

internal sealed class Grain : ISproutEntity
{
    public ulong Id { get; set; }
    public string K { get; set; } = "";
    public string? Etag { get; set; }
    public long N { get; set; }
}

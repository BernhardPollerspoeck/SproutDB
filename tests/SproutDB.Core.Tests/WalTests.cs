namespace SproutDB.Core.Tests;

public class WalTests : IDisposable
{
    private readonly string _tempDir;

    public WalTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sproutdb-wal-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    // ── WAL write + replay: data survives engine restart ─────

    [Fact]
    public void Replay_RecoversInsertedData()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        // First engine: create db + table, insert data
        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice', age: 28}", "testdb");
            engine.ExecuteOne("upsert users {name: 'Bob', age: 35}", "testdb");
        }

        // Second engine: WAL replays, data should be there
        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users", "testdb");

            Assert.Equal(SproutOperation.Get, r.Operation);
            Assert.Equal(2, r.Data?.Count);
            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Equal("Bob", r.Data?[1]["name"]);
        }
    }

    [Fact]
    public void Replay_PreservesAutoIncrementIds()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice'}", "testdb"); // id=1
            engine.ExecuteOne("upsert users {name: 'Bob'}", "testdb");   // id=2
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users select _id, name", "testdb");

            Assert.Equal((ulong)1, r.Data?[0]["_id"]);
            Assert.Equal((ulong)2, r.Data?[1]["_id"]);
        }
    }

    [Fact]
    public void Replay_ContinuesAutoIncrementAfterRestart()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice'}", "testdb"); // id=1
        }

        using (var engine = new SproutEngine(dataDir))
        {
            // After replay, next insert should get id=2
            var r = engine.ExecuteOne("upsert users {name: 'Bob'}", "testdb");
            Assert.Equal((ulong)2, r.Data?[0]["_id"]);
        }
    }

    [Fact]
    public void Replay_HandlesUpdatesCorrectly()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100, score sint)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice', score: 100}", "testdb");
            engine.ExecuteOne("upsert users {_id: 1, score: 200}", "testdb"); // update
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users select name, score", "testdb");

            Assert.Single(r.Data ?? []);
            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Equal(200, r.Data?[0]["score"]);
        }
    }

    [Fact]
    public void Replay_HandlesAddColumn()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice'}", "testdb");
            engine.ExecuteOne("add column users.score sint", "testdb");
            engine.ExecuteOne("upsert users {_id: 1, score: 42}", "testdb");
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users select name, score", "testdb");

            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Equal(42, r.Data?[0]["score"]);
        }
    }

    [Fact]
    public void Replay_CreateTableIsIdempotent()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
        }

        // Replay should not crash even though table already exists on disk
        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users", "testdb");
            Assert.Equal(SproutOperation.Get, r.Operation);
        }
    }

    [Fact]
    public void Replay_WalIsTruncatedAfterReplay()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice'}", "testdb");
        }

        // After replay + truncation, WAL file should be empty
        using (var engine = new SproutEngine(dataDir))
        {
            // Trigger replay by accessing the database
            engine.ExecuteOne("get users", "testdb");
        }

        // Check WAL file is empty
        var walPath = Path.Combine(dataDir, "testdb", "_wal");
        Assert.True(File.Exists(walPath));
        Assert.Equal(0, new FileInfo(walPath).Length);
    }

    [Fact]
    public void Replay_EmptyWal_NoErrors()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
        }

        // First restart: replays and truncates
        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("get users", "testdb");
        }

        // Second restart: WAL is empty, should work fine
        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users", "testdb");
            Assert.Equal(SproutOperation.Get, r.Operation);
            Assert.Empty(r.Data ?? []);
        }
    }

    [Fact]
    public void Replay_NullValuesPreserved()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100, age ubyte)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice'}", "testdb"); // age=null
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users select name, age", "testdb");

            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Null(r.Data?[0]["age"]);
        }
    }

    [Fact]
    public void Replay_DefaultValuesPreserved()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100, active bool default true)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice'}", "testdb"); // active=true (default)
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users select name, active", "testdb");

            Assert.Equal("Alice", r.Data?[0]["name"]);
            Assert.Equal(true, r.Data?[0]["active"]);
        }
    }

    [Fact]
    public void Replay_MultipleTablesInSameDatabase()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
            engine.ExecuteOne("create table orders (total sint)", "testdb");
            engine.ExecuteOne("upsert users {name: 'Alice'}", "testdb");
            engine.ExecuteOne("upsert orders {total: 500}", "testdb");
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var users = engine.ExecuteOne("get users", "testdb");
            var orders = engine.ExecuteOne("get orders", "testdb");

            Assert.Equal("Alice", users.Data?[0]["name"]);
            Assert.Equal(500, orders.Data?[0]["total"]);
        }
    }

    [Fact]
    public void Replay_UpsertIsIdempotent()
    {
        var dataDir = Path.Combine(_tempDir, "data");

        ulong insertedId;
        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
            var r = engine.ExecuteOne("upsert users {name: 'Alice'}", "testdb");
            insertedId = (ulong)(r.Data?[0]["_id"] ?? 0UL);
        }

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users select _id, name", "testdb");

            Assert.Single(r.Data ?? []);
            Assert.Equal(insertedId, r.Data?[0]["_id"]);
            Assert.Equal("Alice", r.Data?[0]["name"]);
        }
    }

    // ── Crash replay: WAL entries must be single statements ──

    [Fact]
    public void Replay_AfterCrash_MultiQueryBatchAndTrailingSemicolon_AreRecovered()
    {
        var dataDir = Path.Combine(_tempDir, "data");
        var crashDir = Path.Combine(_tempDir, "crash");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
        }

        // Schema-only state as it was on disk before the writes below
        CopyDirectory(dataDir, crashDir);

        byte[] walBytes;
        var settings = new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        };
        using (var engine = new SproutEngine(settings))
        {
            engine.Execute("upsert users {name: 'a'}; upsert users {name: 'b'}", "testdb");
            engine.Execute("upsert users {name: 'c'};", "testdb");
            walBytes = ReadWalBytes(Path.Combine(dataDir, "testdb", "_wal"));
        }

        // Crash: data files never got flushed, only the WAL survived
        File.WriteAllBytes(Path.Combine(crashDir, "testdb", "_wal"), walBytes);

        using (var engine = new SproutEngine(crashDir))
        {
            var r = engine.ExecuteOne("get users select name", "testdb");
            var names = (r.Data ?? []).Select(row => row["name"] as string).ToList();
            Assert.Equal(["a", "b", "c"], names);
        }
    }

    [Fact]
    public void Replay_AfterCrash_UpsertOn_UpdatesAndInserts()
    {
        var dataDir = Path.Combine(_tempDir, "data");
        var crashDir = Path.Combine(_tempDir, "crash");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table gs (k string 64, etag string 16)", "testdb");
            engine.ExecuteOne("upsert gs {k: 'old', etag: 'E1'}", "testdb");
        }

        CopyDirectory(dataDir, crashDir);

        byte[] walBytes;
        var settings = new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        };
        using (var engine = new SproutEngine(settings))
        {
            engine.ExecuteOne("upsert gs {k: 'old', etag: 'E2'} on k", "testdb");
            engine.ExecuteOne("upsert gs {k: 'new', etag: 'N1'} on k", "testdb");
            walBytes = ReadWalBytes(Path.Combine(dataDir, "testdb", "_wal"));
        }

        File.WriteAllBytes(Path.Combine(crashDir, "testdb", "_wal"), walBytes);

        using (var engine = new SproutEngine(crashDir))
        {
            var r = engine.ExecuteOne("get gs select _id, k, etag order by _id", "testdb");
            var rows = r.Data ?? [];
            Assert.Equal(2, rows.Count);
            Assert.Equal((ulong)1, rows[0]["_id"]);
            Assert.Equal("E2", rows[0]["etag"]);
            Assert.Equal((ulong)2, rows[1]["_id"]);
            Assert.Equal("new", rows[1]["k"]);
            Assert.Equal("N1", rows[1]["etag"]);
        }
    }

    [Fact]
    public void Replay_OnTopOfAlreadyPersistedRows_IsIdempotent()
    {
        // Data files may already contain some of the WAL's writes (the OS
        // flushes MMF pages whenever it likes). Replay must not duplicate rows.
        var dataDir = Path.Combine(_tempDir, "data");

        byte[] walBytes;
        var settings = new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        };
        using (var engine = new SproutEngine(settings))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table gs (k string 64, etag string 16)", "testdb");
            engine.ExecuteOne("upsert gs {k: 'a', etag: 'E1'} on k", "testdb");
            engine.ExecuteOne("upsert gs {k: 'b', etag: 'E1'}", "testdb");
            walBytes = ReadWalBytes(Path.Combine(dataDir, "testdb", "_wal"));
        }

        // Clean shutdown persisted everything — now pretend the WAL survived too
        File.WriteAllBytes(Path.Combine(dataDir, "testdb", "_wal"), walBytes);

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get gs select _id, k order by _id", "testdb");
            var rows = r.Data ?? [];
            Assert.Equal(2, rows.Count);
            Assert.Equal("a", rows[0]["k"]);
            Assert.Equal("b", rows[1]["k"]);
        }
    }

    // ── Crash replay: transactions ───────────────────────────

    /// <summary>
    /// Creates db + table cleanly, snapshots that state as the "crash" directory,
    /// runs <paramref name="writes"/> and returns the WAL as it was before shutdown.
    /// </summary>
    private (string CrashDir, byte[] Wal) RunAndCaptureWal(Action<SproutEngine> writes)
    {
        var dataDir = Path.Combine(_tempDir, "data");
        var crashDir = Path.Combine(_tempDir, "crash");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
        }

        CopyDirectory(dataDir, crashDir);

        var settings = new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        };
        using var engine2 = new SproutEngine(settings);
        writes(engine2);
        return (crashDir, ReadWalBytes(Path.Combine(dataDir, "testdb", "_wal")));
    }

    private static List<(ulong Id, string? Name)> ReplayAndReadUsers(string crashDir, byte[] wal)
    {
        File.WriteAllBytes(Path.Combine(crashDir, "testdb", "_wal"), wal);
        using var engine = new SproutEngine(crashDir);
        var r = engine.ExecuteOne("get users select _id, name order by _id", "testdb");
        return (r.Data ?? []).Select(row => ((ulong)(row["_id"] ?? 0UL), row["name"] as string)).ToList();
    }

    [Fact]
    public void Replay_AfterCrash_CommittedTransaction_KeepsDistinctIds()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
            engine.Execute("atomic; upsert users {name: 'a'}; upsert users {name: 'b'}; commit", "testdb"));

        var users = ReplayAndReadUsers(crashDir, wal);

        Assert.Equal([(1UL, "a"), (2UL, "b")], users);
    }

    [Fact]
    public void Replay_AfterCrash_RolledBackTransaction_IsSkipped()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            var tx = engine.Execute("atomic; upsert users {name: 'tx'}; upsert nonexistent {x: 1}; commit", "testdb");
            Assert.Equal(SproutOperation.Error, tx[0].Operation);
            engine.ExecuteOne("upsert users {name: 'after'}", "testdb");
        });

        var users = ReplayAndReadUsers(crashDir, wal);

        // The rolled-back insert reused no _id: 'after' got 1, exactly as live
        Assert.Equal([(1UL, "after")], users);
    }

    [Fact]
    public void Replay_AfterCrash_TransactionWithoutCommitMarker_IsSkipped()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            engine.ExecuteOne("upsert users {name: 'before'}", "testdb");
            engine.Execute("atomic; upsert users {name: 'a'}; upsert users {name: 'b'}; commit", "testdb");
        });

        // Crash before the marker reached the disk: cut the last entry
        var markerSize = 28 + System.Text.Encoding.UTF8.GetByteCount(SproutEngine.WalCommitMarker);
        var truncated = wal[..^markerSize];

        var users = ReplayAndReadUsers(crashDir, truncated);

        Assert.Equal([(1UL, "before")], users);
    }

    // ── Conditional writes ───────────────────────────────────

    private static List<string> WalQueries(byte[] wal)
    {
        var queries = new List<string>();
        var pos = 0;
        while (pos + 28 <= wal.Length)
        {
            var len = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(wal.AsSpan(pos + 16));
            queries.Add(System.Text.Encoding.UTF8.GetString(wal, pos + 28, len));
            pos += 28 + len;
        }
        return queries;
    }

    [Fact]
    public void Wal_RejectedWrites_AreNotLogged()
    {
        var (_, wal) = RunAndCaptureWal(engine =>
        {
            engine.ExecuteOne("upsert users {name: 'a'}", "testdb");
            engine.ExecuteOne("upsert users {name: 'a'} on name when not exists", "testdb");  // row exists
            engine.ExecuteOne("upsert users {name: 'b'} on name when exists", "testdb");      // row missing
            engine.ExecuteOne("upsert users {nope: 1}", "testdb");                            // unknown column
            engine.ExecuteOne("delete users where name = 'zzz' expect 1", "testdb");          // expectation
            engine.ExecuteOne("delete users where name = 'zzz'", "testdb");                   // nothing matched
        });

        Assert.Equal(["upsert users {name: 'a'}"], WalQueries(wal));
    }

    [Fact]
    public void Replay_ConditionalUpsert_HealsTornRow()
    {
        var dataDir = Path.Combine(_tempDir, "data");
        var crashDir = Path.Combine(_tempDir, "crash");

        using (var engine = new SproutEngine(dataDir))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table gs (k string 64, etag string 16, payload string 100)", "testdb");
            engine.ExecuteOne("upsert gs {k: 'k', etag: 'E1', payload: 'old'}", "testdb");
        }

        CopyDirectory(dataDir, crashDir);

        byte[] walBytes;
        var settings = new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        };
        using (var engine = new SproutEngine(settings))
        {
            var r = engine.ExecuteOne("upsert gs {k: 'k', etag: 'E2', payload: 'new'} on k when etag = 'E1'", "testdb");
            Assert.Null(r.Errors);
            walBytes = ReadWalBytes(Path.Combine(dataDir, "testdb", "_wal"));
        }

        // Torn write: the etag column reached the disk before the crash, payload did not
        using (var engine = new SproutEngine(crashDir))
            engine.ExecuteOne("upsert gs {k: 'k', etag: 'E2'} on k", "testdb");

        File.WriteAllBytes(Path.Combine(crashDir, "testdb", "_wal"), walBytes);

        using (var engine = new SproutEngine(crashDir))
        {
            var r = engine.ExecuteOne("get gs select etag, payload", "testdb");
            var row = Assert.Single(r.Data ?? []);
            Assert.Equal("E2", row["etag"]);
            Assert.Equal("new", row["payload"]);
        }
    }

    [Fact]
    public void Replay_WhenNotExistsOverExpiredRow_FreesAndReinsertsWithSameId()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            engine.ExecuteOne("upsert users {name: 'a', ttl: 1s}", "testdb");
            Thread.Sleep(1100);
            var r = engine.ExecuteOne("upsert users {name: 'a'} on name when not exists", "testdb");
            Assert.Equal((ulong)2, r.Data?[0]["_id"]);
        });

        var users = ReplayAndReadUsers(crashDir, wal);

        Assert.Equal([(2UL, "a")], users);
    }

    [Fact]
    public void Replay_PlainUpsertOnOverExpiredRow_MatchesLiveResult()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            engine.ExecuteOne("upsert users {name: 'a', ttl: 1s}", "testdb");
            Thread.Sleep(1100);
            var r = engine.ExecuteOne("upsert users {name: 'a'} on name", "testdb");
            Assert.Equal((ulong)2, r.Data?[0]["_id"]);
        });

        // On replay the first row gets a fresh 1s TTL and is alive again —
        // the result must still be what happened live: row 1 freed, row 2 inserted
        var users = ReplayAndReadUsers(crashDir, wal);

        Assert.Equal([(2UL, "a")], users);
    }

    [Fact]
    public void Replay_UpdateOfRowThatExpiredSince_StaysAnUpdate()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            engine.ExecuteOne("upsert users {name: 'a', ttl: 1s}", "testdb");
            var r = engine.ExecuteOne("upsert users {name: 'a', ttl: 1h} on name", "testdb"); // alive: update
            Assert.Equal((ulong)1, r.Data?[0]["_id"]);
        });

        Thread.Sleep(1100); // by replay time the first entry's TTL would be over again

        var users = ReplayAndReadUsers(crashDir, wal);

        Assert.Equal([(1UL, "a")], users);
    }

    [Fact]
    public void Replay_CreateTableWithUniqueColumn_EnforcesUniqueAfterReplay()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            engine.ExecuteOne("create table gs (k string 64 unique)", "testdb");
            engine.ExecuteOne("upsert gs {k: 'a'}", "testdb");
        });

        File.WriteAllBytes(Path.Combine(crashDir, "testdb", "_wal"), wal);
        using var engine = new SproutEngine(crashDir);

        Assert.Single(engine.ExecuteOne("get gs", "testdb").Data ?? []);
        Assert.Equal("UNIQUE_VIOLATION", engine.ExecuteOne("upsert gs {k: 'a'}", "testdb").Errors?[0].Code);
    }

    [Fact]
    public void Replay_ManualIndexes_StayManual()
    {
        // Index metrics are persisted on flush only. A crash before that must not
        // turn user-created indexes into auto indexes (which get purged when unused).
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            engine.ExecuteOne("create table gs (k string 64 unique)", "testdb");
            engine.ExecuteOne("create index users.name", "testdb");
            engine.ExecuteOne("create index gs.nope", "testdb"); // fails, but is in the WAL
        });

        File.WriteAllBytes(Path.Combine(crashDir, "testdb", "_wal"), wal);
        using var engine = new SproutEngine(crashDir);

        // Usage after the restart creates the metrics entries
        engine.ExecuteOne("get gs where k = 'a'", "testdb");
        engine.ExecuteOne("get users where name = 'a'", "testdb");

        var gsCol = engine.ExecuteOne("describe gs", "testdb").Schema?.Columns.Single(c => c.Name == "k");
        var usersCol = engine.ExecuteOne("describe users", "testdb").Schema?.Columns.Single(c => c.Name == "name");
        Assert.False(gsCol?.IsAutoIndex);
        Assert.True(gsCol?.IsUnique);
        Assert.False(usersCol?.IsAutoIndex);
        Assert.True(usersCol?.Indexed);
    }

    [Fact]
    public void Wal_StoresBoundQuery_AndReplaysIt()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            var db = engine.GetOrCreateDatabase("testdb");
            db.Query("upsert users {name: @name}", new { name = "it's a\\" });
        });

        Assert.Equal(["upsert users {name: 'it\\'s a\\\\'}"], WalQueries(wal));

        var users = ReplayAndReadUsers(crashDir, wal);
        Assert.Equal([(1UL, "it's a\\")], users);
    }

    [Fact]
    public void Replay_DeleteExpect_IsAppliedWithoutRecheck()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
        {
            engine.ExecuteOne("upsert users {name: 'a'}", "testdb");
            engine.ExecuteOne("upsert users {name: 'b'}", "testdb");
            var r = engine.ExecuteOne("delete users where name = 'a' expect 1", "testdb");
            Assert.Null(r.Errors);
        });

        var users = ReplayAndReadUsers(crashDir, wal);

        Assert.Equal([(2UL, "b")], users);
    }

    [Fact]
    public void Replay_BulkInsert_OnTopOfPersistedRows_CreatesNoDuplicates()
    {
        // Process crash: the OS keeps the MMF writes, so data files are current
        // AND the WAL is replayed. A bulk insert must not insert its rows twice.
        var dataDir = Path.Combine(_tempDir, "data");

        byte[] walBytes;
        var settings = new SproutEngineSettings
        {
            DataDirectory = dataDir,
            FlushInterval = Timeout.InfiniteTimeSpan,
        };
        using (var engine = new SproutEngine(settings))
        {
            engine.ExecuteOne("create database", "testdb");
            engine.ExecuteOne("create table users (name string 100)", "testdb");
            engine.ExecuteOne("upsert users {name: 'first'}", "testdb");
            engine.ExecuteOne("upsert users [{name: 'a'}, {_id: 1, name: 'first2'}, {name: 'b'}]", "testdb");
            walBytes = ReadWalBytes(Path.Combine(dataDir, "testdb", "_wal"));
        }

        File.WriteAllBytes(Path.Combine(dataDir, "testdb", "_wal"), walBytes);

        using (var engine = new SproutEngine(dataDir))
        {
            var r = engine.ExecuteOne("get users select _id, name order by _id", "testdb");
            var users = (r.Data ?? []).Select(row => ((ulong)(row["_id"] ?? 0UL), row["name"] as string)).ToList();
            Assert.Equal([(1UL, "first2"), (2UL, "a"), (3UL, "b")], users);
        }
    }

    [Fact]
    public void Replay_AfterCrash_BulkInsert_KeepsIds()
    {
        var (crashDir, wal) = RunAndCaptureWal(engine =>
            engine.ExecuteOne("upsert users [{name: 'a'}, {name: 'b'}]", "testdb"));

        var users = ReplayAndReadUsers(crashDir, wal);

        Assert.Equal([(1UL, "a"), (2UL, "b")], users);
    }

    private static byte[] ReadWalBytes(string walPath)
    {
        // The engine holds the WAL open with FileShare.Read
        using var fs = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var ms = new MemoryStream();
        fs.CopyTo(ms);
        return ms.ToArray();
    }

    private static void CopyDirectory(string source, string target)
    {
        Directory.CreateDirectory(target);
        foreach (var file in Directory.GetFiles(source))
            File.Copy(file, Path.Combine(target, Path.GetFileName(file)));
        foreach (var dir in Directory.GetDirectories(source))
            CopyDirectory(dir, Path.Combine(target, Path.GetFileName(dir)));
    }
}

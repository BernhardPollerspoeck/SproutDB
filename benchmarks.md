# SproutDB Benchmarks

Gemessen auf: AMD Ryzen 7 5800X, 16 logical cores, .NET 9.0.9, Windows 11
BenchmarkDotNet v0.14.0, ShortRunJob (3 Iterationen)

---

## Insert Pipeline Breakdown

Zerlegt einen einzelnen Insert in seine Phasen um Bottlenecks zu identifizieren.

| Phase | Mean | Allocated |
|---|---|---|
| 1. Parse only (3 fields) | 224 ns | 1,064 B |
| 1. Parse only (5 fields) | 376 ns | 1,848 B |
| 2. WAL append (buffer only) | 3,216 ns | 72 B |
| 3. MMF write only (3 fields) | 3,412 ns | 1,352 B |
| 3. MMF write only (5 fields) | 3,613 ns | 1,520 B |
| **4. Full pipeline (3 fields)** | **8,469 ns** | **3,336 B** |
| **4. Full pipeline (5 fields)** | **7,569 ns** | **4,328 B** |

## Insert Throughput

Misst die Durchsatzrate einzelner Inserts (100 Inserts pro Invocation, OperationsPerInvoke=100).

| Benchmark | Mean | Allocated | Throughput |
|---|---|---|---|
| Insert: single row (3 fields) | 6.67 us | 3.24 KB | ~150,000/sec |
| Insert: single row (5 fields) | 7.15 us | 4.21 KB | ~140,000/sec |
| Insert: single row (empty) | 6.18 us | 2.38 KB | ~162,000/sec |

## Upsert (Insert + Update)

Misst Insert und Update getrennt. Updates laufen gegen 1000 vorher eingefügte Rows.

| Benchmark | Mean | Allocated |
|---|---|---|
| Insert: 5 fields | 6.32 us | 4.93 KB |
| Insert: empty record | 5.56 us | 2.61 KB |
| Update: 1 field | 6.32 us | 3.01 KB |
| Update: 3 fields | 6.07 us | 3.76 KB |

## Get (Read)

Misst Get-Operationen bei verschiedenen Tabellengroessen (100, 10k, 1M Rows).

| Benchmark | RowCount | Mean | Allocated |
|---|---|---|---|
| Get: all columns | 100 | 67 us | 51 KB |
| Get: select 2 columns | 100 | 42 us | 24 KB |
| Get: select id only | 100 | 31 us | 14 KB |
| Get: all columns | 10,000 | 10,142 us | 5,090 KB |
| Get: select 2 columns | 10,000 | 3,606 us | 2,349 KB |
| Get: select id only | 10,000 | 1,730 us | 2,601 KB |
| Get: all columns | 1,000,000 | 2,115,639 us (~2.1s) | ~977 MB |
| Get: select 2 columns | 1,000,000 | 810,779 us (~0.8s) | ~412 MB |
| Get: select id only | 1,000,000 | 399,049 us (~0.4s) | ~245 MB |

## End-to-End (Error Paths)

Misst die vollstaendige Parse+Execute Pipeline fuer schnelle Error-Pfade.

| Benchmark | Mean | Allocated |
|---|---|---|
| E2E: create database (error: exists) | 12,355 ns | 880 B |
| E2E: unknown command | 279 ns | 1,944 B |
| E2E: invalid db name | 64 ns | 416 B |

---

## Zusammenfassung

- **Insert**: ~6-7 us pro Row = ~150,000 inserts/sec
- **Update**: ~6 us pro Row = ~165,000 updates/sec
- **Get 1M Rows (all columns)**: ~2.1 Sekunden
- **Get 1M Rows (select 2)**: ~0.8 Sekunden
- **Get 1M Rows (id only)**: ~0.4 Sekunden
- **1M Inserts (Setup-Zeit)**: ~6.5 Sekunden
- **Error-Pfade**: 64-279 ns (fast sofort)

### Optimierungen

1. **WAL Group Commit** (50ms): fsync wird gebatched statt pro Write. Setting: `WalSyncInterval`.
2. **O(1) FindNextPlace**: Index-Scan nur einmal beim Open, danach In-Memory Counter.
3. **Syscall Elimination**: `Directory.Exists` durch In-Memory Cache ersetzt, `Path.Combine` nur 1x.
4. **Binary WAL ID**: Auto-ID wird als Binary-Feld im WAL-Header gespeichert statt Query-String-Rebuild.

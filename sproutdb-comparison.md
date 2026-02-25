# SproutDB vs. The World

Ehrlicher Vergleich. SproutDB gewinnt in manchen Bereichen, verliert in anderen. Kein Marketing, nur Fakten.

---

## Legende

- ✅ Ja / nativ unterstützt
- ⚡ Besonders stark (Architektur-Vorteil)
- ⚠️ Eingeschränkt / mit Workaround
- ❌ Nein / nicht möglich
- 🔌 Nur mit Plugin/Extension

---

## Direkte Konkurrenten (Embedded / Lightweight)

|  | **SproutDB** | **LiteDB** | **SQLite** | **RavenDB Embedded** | **DuckDB** |
|---|---|---|---|---|---|
| **Sprache** | C# | C# | C | C# | C++ |
| **.NET native** | ⚡ Ja | ✅ Ja | ⚠️ Wrapper | ✅ Ja | ⚠️ Wrapper |
| **NuGet Package** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Embedded Mode** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Network Mode** | ⚡ Built-in HTTP + SignalR | ❌ | ❌ | ✅ HTTP | ❌ |
| **Query Language** | ⚡ Human-readable | LINQ / BsonExpression | SQL | RQL / LINQ | SQL |
| **Realtime Notifications** | ⚡ SignalR built-in | ❌ | ❌ | ✅ Subscriptions | ❌ |
| **Schema** | Typed, Fixed-Size | Schemaless (BSON) | Typed | Schemaless (JSON) | Typed |
| **Storage Model** | ⚡ Column-per-File | Document (Single File) | Page-based (Single File) | Document | Column-oriented |
| **Concurrent Reads** | ⚡ Lock-free MMF | ⚠️ Single-Reader | ⚠️ WAL Mode nötig | ✅ | ✅ |
| **Write Throughput** | ⚠️ Single-Writer | ⚠️ Single-Writer | ⚠️ Single-Writer | ✅ | ✅ Batch-optimiert |
| **Schema Evolution** | ⚡ File-Op (instant) | ✅ Schemaless | ⚠️ ALTER TABLE | ✅ Schemaless | ⚠️ ALTER TABLE |
| **Migrations** | ⚡ Built-in, per DB | ❌ | ❌ | ❌ | ❌ |
| **Multi-Tenant** | ⚡ DB = Folder = ZIP | ⚠️ Multi-File | ⚠️ Multi-File | ✅ | ⚠️ Multi-File |
| **Auto-Index** | ⚡ Usage-based | ❌ | ❌ | ✅ Auto-Indexes | ❌ |
| **Joins** | ✅ follow Syntax | ⚠️ Include Refs | ✅ SQL Joins | ✅ Includes | ✅ SQL Joins |
| **Aggregation** | ✅ | ⚠️ LINQ | ✅ SQL | ✅ MapReduce | ⚡ OLAP-optimiert |
| **ACID** | ✅ WAL + fsync | ✅ | ✅ | ✅ | ✅ |
| **Backup** | ⚡ ZIP the folder | Copy File | Copy File | Snapshot | Copy File |
| **Lizenz** | MIT | MIT | Public Domain | Dual (AGPL/Comm) | MIT |
| **Use Case** | .NET Apps, SaaS, Multi-Tenant | Embedded .NET | Everywhere | .NET Enterprise | Analytics, OLAP |

### Wo SproutDB gewinnt
- Einzige embedded DB mit built-in HTTP Server + SignalR
- Schema-Änderungen sind File-Operationen statt Table Rebuilds
- Annotated Error Queries (kein anderer hat das)
- Multi-Tenant = Folder = ZIP (simpelster Backup/Restore)
- Migrations built-in, kein externes Tool

### Wo SproutDB verliert
- Write Throughput: Single-Writer limitiert (bewusste Design-Entscheidung)
- Keine Volltextsuche
- Keine Schemaless Option
- DuckDB ist für Analytics überlegen
- RavenDB hat mehr Enterprise Features

---

## Standard-Datenbanken (Die "Warum nicht einfach X?" Frage)

|  | **SproutDB** | **PostgreSQL** | **MongoDB** | **MySQL** | **SQL Server** |
|---|---|---|---|---|---|
| **Deployment** | ⚡ NuGet Package | Container/Install | Container/Install | Container/Install | Container/Install |
| **Ops-Overhead** | ⚡ Null | Hoch | Mittel | Mittel | Hoch |
| **Embedded Mode** | ✅ | ❌ | ❌ | ❌ | ❌ (LocalDB ≠ embedded) |
| **Network Mode** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **.NET native** | ⚡ | ⚠️ Npgsql | ⚠️ Driver | ⚠️ Connector | ⚠️ SqlClient |
| **Realtime** | ⚡ SignalR | ⚠️ LISTEN/NOTIFY | ⚠️ Change Streams | ❌ | ⚠️ Service Broker |
| **Query Language** | Human-readable | SQL | MQL (JSON) | SQL | T-SQL |
| **Schema Evolution** | ⚡ Instant | ⚠️ ALTER TABLE (Locks) | ✅ Schemaless | ⚠️ ALTER TABLE | ⚠️ ALTER TABLE |
| **Concurrent Reads** | ⚡ Lock-free | ⚡ MVCC | ⚡ Lock-free | ✅ | ✅ |
| **Write Throughput** | ⚠️ Single-Writer | ⚡⚡ | ⚡⚡ | ⚡⚡ | ⚡⚡ |
| **Bulk Operations** | ⚠️ | ⚡⚡ | ⚡⚡ | ⚡⚡ | ⚡⚡ |
| **Transactions** | ⚠️ Single-Op | ⚡ Multi-Statement | ✅ Multi-Doc | ✅ | ⚡ Multi-Statement |
| **Joins** | ✅ Basic | ⚡ Komplex | ⚠️ $lookup | ✅ | ⚡ Komplex |
| **Stored Procedures** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **Replication** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **Horizontal Scaling** | ❌ | 🔌 Citus | ✅ Sharding | 🔌 Vitess | ⚠️ Always On |
| **Community/Ecosystem** | Klein | ⚡ Riesig | ⚡ Riesig | ⚡ Riesig | ⚡ Riesig |
| **Lizenz** | MIT | PostgreSQL (frei) | SSPL | GPL | Kommerziell |

### Wann SproutDB statt Postgres/Mongo
- Du willst keinen separaten DB-Server betreiben
- Dein Projekt deployed on-premise und der Kunde hat kein DB-Team
- Du brauchst Embedded + Optional-Networked
- Du willst Realtime ohne Extra-Infrastruktur
- Dein Projekt ist klein bis mittel (<10 Mio Rows pro Table)

### Wann Postgres/Mongo statt SproutDB
- Du brauchst hohen Write-Throughput
- Du brauchst komplexe Transaktionen über mehrere Tabellen
- Du brauchst Replication/Sharding
- Du brauchst Stored Procedures
- Du hast ein Ops-Team das die DB betreut
- Du brauchst ein riesiges Ecosystem an Tools und Libraries

---

## Cloud-Datenbanken

|  | **SproutDB** | **CosmosDB** | **DynamoDB** | **PlanetScale** | **Supabase** |
|---|---|---|---|---|---|
| **Self-hosted** | ⚡ Ja | ❌ Azure only | ❌ AWS only | ⚠️ Vitess self-host | ✅ |
| **Vendor Lock-in** | ⚠️ Eigene Query Language (LINQ als Standard-Ausweg) | Hoch | Hoch | Mittel | Niedrig |
| **Kosten bei 0 Traffic** | ⚡ 0€ | ~25€/mo | ~25€/mo | 0€ (Free Tier) | 0€ (Free Tier) |
| **Kosten bei Scale** | ⚡ Server-Kosten only | Hoch (RU-basiert) | Hoch (Capacity Units) | Mittel | Mittel |
| **Offline-fähig** | ⚡ Ja | ❌ | ❌ | ❌ | ❌ |
| **Latenz** | ⚡ In-Process: 0 | ~5-50ms | ~5-20ms | ~10-50ms | ~10-50ms |
| **Global Distribution** | ❌ | ⚡ Multi-Region | ⚡ Multi-Region | ✅ | ⚠️ |

### Wann SproutDB statt Cloud
- Du willst nicht von einem Cloud-Provider abhängig sein
- Du deployest on-premise oder edge
- Du willst volle Kontrolle über Kosten
- Du brauchst Offline-Fähigkeit
- Latenz ist kritisch (In-Process = 0 Network Overhead)

---

## Exoten & Spezialisten

|  | **SproutDB** | **Redis** | **ClickHouse** | **Neo4j** | **InfluxDB** | **Elasticsearch** | **CockroachDB** |
|---|---|---|---|---|---|---|---|
| **Kategorie** | General Purpose | Key-Value / Cache | Column Analytics | Graph | Time Series | Search | NewSQL Distributed |
| **Embedded** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Networked** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Sweet Spot** | .NET Apps | Caching, Pub/Sub | Analytics, Logs | Relationships | Metrics, IoT | Fulltext Search | Global Scale SQL |
| **Read Speed** | ⚡ MMF Direct | ⚡⚡ In-Memory | ⚡⚡ Columnar Scan | ✅ | ✅ | ⚡ Inverted Index | ✅ |
| **Write Speed** | ⚠️ Single-Writer | ⚡⚡ In-Memory | ⚡⚡ Batch Insert | ✅ | ⚡⚡ Append-only | ✅ | ✅ |
| **Persistence** | ✅ Disk-first | ⚠️ Optional (AOF/RDB) | ✅ | ✅ | ✅ | ✅ | ✅ |
| **.NET Support** | ⚡ Native | ⚠️ StackExchange.Redis | ⚠️ ClickHouse.Client | ⚠️ Neo4j.Driver | ⚠️ Client | ⚠️ NEST/Elastic.Clients | ⚠️ Npgsql |
| **Realtime** | ⚡ SignalR | ✅ Pub/Sub | ❌ | ❌ | ❌ | ❌ | ⚠️ Changefeeds |
| **SproutDB ersetzt das?** | — | ❌ Anderer Use Case | ❌ Anderer Use Case | ❌ Anderer Use Case | ❌ Anderer Use Case | ❌ Anderer Use Case | ❌ Anderer Use Case |

> SproutDB ist General Purpose. Es ersetzt keine Spezialisten-DBs.
> Redis für Caching, ClickHouse für Analytics, Neo4j für Graphen, InfluxDB für Metriken, Elasticsearch für Suche — das sind andere Probleme.

---

## Performance-Vergleich (Architektur-basiert)

| Szenario | SproutDB | Postgres | MongoDB | LiteDB | SQLite |
|---|---|---|---|---|---|
| Single Row by ID | ⚡⚡⚡⚡ | ⚡⚡⚡ | ⚡⚡⚡ | ⚡⚡ | ⚡⚡⚡ |
| Select wenige Spalten | ⚡⚡⚡⚡ | ⚡⚡⚡ | ⚡⚡ | ⚡ | ⚡⚡⚡ |
| Write Single Row | ⚡⚡ | ⚡⚡⚡ | ⚡⚡⚡ | ⚡⚡⚡ | ⚡⚡ |
| Bulk Write 100k Rows | ⚡ | ⚡⚡⚡⚡ | ⚡⚡⚡⚡ | ⚡⚡ | ⚡⚡⚡ |
| Full Table Scan | ⚡⚡ | ⚡⚡⚡ | ⚡⚡ | ⚡ | ⚡⚡⚡ |
| Schema Change | ⚡⚡⚡⚡ | ⚡ | ⚡⚡⚡⚡ | ⚡⚡⚡ | ⚡ |
| Concurrent Reads | ⚡⚡⚡⚡ | ⚡⚡⚡⚡ | ⚡⚡⚡⚡ | ⚡ | ⚡⚡ |
| Realtime Notifications | ⚡⚡⚡⚡ | ⚡ | ⚡⚡ | ❌ | ❌ |
| Complex Joins | ⚡⚡ | ⚡⚡⚡⚡ | ⚡ | ⚠️ | ⚡⚡⚡⚡ |
| Aggregation | ⚡⚡ | ⚡⚡⚡⚡ | ⚡⚡⚡ | ⚡ | ⚡⚡⚡ |
| Startup Time | ⚡⚡⚡ | ⚡ | ⚡ | ⚡⚡⚡⚡ | ⚡⚡⚡⚡ |

> Werte sind architektur-basierte Einschätzungen, keine Benchmarks. Echte Benchmarks kommen wenn Phase 1 steht.

### SproutDB ist schneller bei:
- **ID Lookups:** Direkter Offset-Zugriff, kein B-Tree Traversal
- **Spalten-Reads:** Column-per-File, nur relevante Daten lesen
- **Schema Changes:** File-Operation statt Table Rebuild
- **Concurrent Reads:** Lock-free MMF, kein Lock-Contention
- **Realtime:** Built-in, kein Umweg

### SproutDB ist langsamer bei:
- **Write Throughput:** Single-Writer Queue, bewusste Design-Entscheidung
- **Bulk Operations:** Kein Batch-Optimierung, jeder Write durch die Queue
- **Complex Joins:** Kein Query Optimizer auf dem Level von Postgres
- **Full Table Scan:** Row-based DBs können komplette Rows schneller sequentiell lesen
- **Aggregation:** Keine SIMD-Optimierung wie DuckDB/ClickHouse

---

## Zusammenfassung

**SproutDB ist für:**
- .NET Entwickler die eine DB ohne Ops wollen
- Projekte die embedded UND networked brauchen
- On-premise / Edge Deployments
- Multi-Tenant SaaS mit einfachem Tenant-Management
- Apps die Realtime-Updates brauchen ohne Extra-Infrastruktur
- Kleine bis mittlere Datenmengen (<10 Mio Rows pro Table)

**SproutDB ist NICHT für:**
- Hoher Write-Throughput (>10.000 Writes/Sekunde sustained)
- Big Data / Analytics (ClickHouse, DuckDB)
- Globale Distribution (CockroachDB, Spanner)
- Volltext-Suche (Elasticsearch)
- Graph-Queries (Neo4j)
- Caching (Redis)
- Zeitreihen (InfluxDB)
- Enterprise mit dedizierten DB-Teams (Postgres, SQL Server)

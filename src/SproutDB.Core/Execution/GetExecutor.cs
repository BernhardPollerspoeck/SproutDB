using SproutDB.Core.Parsing;
using SproutDB.Core.Storage;

namespace SproutDB.Core.Execution;

internal static class GetExecutor
{
    public static SproutResponse Execute(string query, TableHandle table, GetQuery q, int defaultPageSize,
        Func<string, TableHandle?>? tableResolver = null)
    {
        // Validate select columns — collect all unknown columns
        List<SproutError>? validationErrors = null;

        if (q.Select is not null)
        {
            foreach (var col in q.Select)
            {
                if (col.Name != "_id" && !table.HasColumn(col.Name))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{col.Name}' does not exist", Position = col.Position, Length = col.Length });
                }
            }
        }

        // Validate computed select columns
        if (q.ComputedSelect is not null)
        {
            foreach (var comp in q.ComputedSelect)
            {
                if (comp.LeftColumn != "_id" && !table.HasColumn(comp.LeftColumn))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{comp.LeftColumn}' does not exist", Position = comp.LeftPosition, Length = comp.LeftLength });
                }
                if (comp.RightColumn is not null && comp.RightColumn != "_id" && !table.HasColumn(comp.RightColumn))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{comp.RightColumn}' does not exist", Position = comp.RightPosition, Length = comp.RightLength });
                }
            }
        }

        // Validate where tree
        if (q.Where is not null)
        {
            var whereErrors = WhereEngine.ValidateWhereNode(table, q.Where);
            validationErrors = WhereEngine.MergeErrors(validationErrors, whereErrors);
        }

        // Validate order by columns (skip when group by or computed fields are active — result columns may be virtual)
        if (q.OrderBy is not null && q.GroupBy is null)
        {
            // Collect computed aliases so we don't reject them as unknown
            HashSet<string>? computedAliases = null;
            if (q.ComputedSelect is not null)
            {
                computedAliases = new HashSet<string>(q.ComputedSelect.Count);
                foreach (var comp in q.ComputedSelect)
                    computedAliases.Add(comp.Alias);
            }

            foreach (var col in q.OrderBy)
            {
                if (col.Name != "_id" && !table.HasColumn(col.Name)
                    && (computedAliases is null || !computedAliases.Contains(col.Name)))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{col.Name}' does not exist", Position = col.Position, Length = col.Length });
                }
            }
        }

        // Validate aggregate column
        if (q.Aggregate.HasValue && q.AggregateColumn is not null)
        {
            if (q.AggregateColumn != "_id" && !table.HasColumn(q.AggregateColumn))
            {
                validationErrors ??= [];
                validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{q.AggregateColumn}' does not exist", Position = q.AggregateColumnPosition, Length = q.AggregateColumnLength });
            }
            else if (q.Aggregate is AggregateFunction.Sum or AggregateFunction.Avg
                     && q.AggregateColumn != "_id" && !IsNumericColumn(table, q.AggregateColumn))
            {
                validationErrors ??= [];
                validationErrors.Add(new SproutError { Code = ErrorCodes.TYPE_MISMATCH, Message = $"'{AggregateName(q.Aggregate.Value)}' can only be used on numeric columns", Position = q.AggregateColumnPosition, Length = q.AggregateColumnLength });
            }
        }

        // Validate follow clauses
        if (q.Follow is not null && tableResolver is not null)
        {
            foreach (var follow in q.Follow)
            {
                var targetTable = tableResolver(follow.TargetTable);
                if (targetTable is null)
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_TABLE, Message = $"table '{follow.TargetTable}' does not exist", Position = follow.TargetTablePosition, Length = follow.TargetTableLength });
                    continue;
                }

                // Validate source column exists on appropriate table
                var sourceTable = follow.SourceTable == q.Table ? table : tableResolver(follow.SourceTable);
                if (follow.SourceColumn != "_id" && (sourceTable is null || !sourceTable.HasColumn(follow.SourceColumn)))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{follow.SourceColumn}' does not exist on '{follow.SourceTable}'", Position = follow.SourceColumnPosition, Length = follow.SourceColumnLength });
                }

                // Validate target column exists
                if (follow.TargetColumn != "_id" && !targetTable.HasColumn(follow.TargetColumn))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{follow.TargetColumn}' does not exist on '{follow.TargetTable}'", Position = follow.TargetColumnPosition, Length = follow.TargetColumnLength });
                }

                // Validate follow where columns
                if (follow.Where is not null)
                {
                    var followWhereErrors = WhereEngine.ValidateWhereNode(targetTable, follow.Where);
                    validationErrors = WhereEngine.MergeErrors(validationErrors, followWhereErrors);
                }
            }
        }

        // Validate group by columns
        if (q.GroupBy is not null)
        {
            foreach (var col in q.GroupBy)
            {
                if (col.Name != "_id" && !table.HasColumn(col.Name))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{col.Name}' does not exist", Position = col.Position, Length = col.Length });
                }
            }
        }

        if (validationErrors is not null)
            return ResponseHelper.Errors(query, validationErrors);

        // Grouped path: aggregate or count with group by
        if (q.GroupBy is not null)
        {
            return ExecuteGrouped(table, q);
        }

        // Aggregate path: compute aggregate directly, skip normal row projection
        if (q.Aggregate.HasValue && q.AggregateColumn is not null)
        {
            return ExecuteAggregate(table, q);
        }

        // Prepare where filter
        var filter = WhereEngine.PrepareFilter(table, q.Where);

        // If computed fields or follow source columns reference columns not in select, include them temporarily
        var extraColumns = ResolveExtraColumnsForComputed(q);
        var followExtras = ResolveExtraColumnsForFollow(q);
        extraColumns = MergeExtras(extraColumns, followExtras);
        var effectiveSelect = MergeSelectWithExtra(q.Select, extraColumns);

        // Snapshot now for TTL filtering (once per query, not per row)
        var nowMs = table.HasTtl ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;

        // Try B-Tree shortcut for simple WHERE conditions
        var btreeResult = TryBTreeLookup(table, q.Where, filter);
        var data = btreeResult is not null
            ? ReadRowsByPlaces(table, effectiveSelect, q.ExcludeSelect, btreeResult, filter, nowMs)
            : ReadRows(table, effectiveSelect, q.ExcludeSelect, filter, nowMs);

        // Compute and add computed field values
        if (q.ComputedSelect is not null)
        {
            ApplyComputedColumns(data, q.ComputedSelect, table);

            // Remove extra columns that were only needed for computation
            if (extraColumns is not null)
            {
                foreach (var row in data)
                {
                    foreach (var extra in extraColumns)
                        row.Remove(extra.Name);
                }
            }
        }

        // Distinct: deduplicate based on projected column values
        if (q.IsDistinct)
        {
            var seen = new HashSet<string>();
            var distinct = new List<Dictionary<string, object?>>(data.Count);
            foreach (var row in data)
            {
                var key = BuildDistinctKey(row);
                if (seen.Add(key))
                    distinct.Add(row);
            }
            data = distinct;
        }

        // Order by
        if (q.OrderBy is not null && q.OrderBy.Count > 0)
        {
            var orderColumns = q.OrderBy;
            data.Sort((a, b) =>
            {
                foreach (var col in orderColumns)
                {
                    a.TryGetValue(col.Name, out var valA);
                    b.TryGetValue(col.Name, out var valB);
                    var cmp = CompareValues(valA, valB);
                    if (col.Descending) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }

        // Follow (join): flat LEFT JOIN — each follow expands rows
        if (q.Follow is not null && tableResolver is not null)
        {
            foreach (var follow in q.Follow)
            {
                var targetTable = tableResolver(follow.TargetTable);
                if (targetTable is null) continue;

                var targetFilter = WhereEngine.PrepareFilter(targetTable, follow.Where);
                data = ExecuteFollow(data, follow, targetTable, targetFilter, q.Table);
            }

            // Remove extra columns that were only needed for follow join resolution
            if (followExtras is not null)
            {
                foreach (var row in data)
                {
                    foreach (var extra in followExtras)
                        row.Remove(extra.Name);
                }
            }

            // Remove follow _id columns that were not explicitly selected
            foreach (var follow in q.Follow)
            {
                if (follow.Select is null) continue;
                var hasId = follow.Select.Exists(s => s.Name == "_id");
                if (hasId) continue;

                var key = $"{follow.Alias}._id";
                foreach (var row in data)
                    row.Remove(key);
            }
        }

        // Count: return count only
        if (q.IsCount)
        {
            return new SproutResponse
            {
                Operation = SproutOperation.Get,
                Data = [],
                Affected = data.Count,
            };
        }

        // Limit (explicit limit disables auto-paging)
        if (q.Limit.HasValue)
        {
            data = data.Take(q.Limit.Value).ToList();
            return new SproutResponse
            {
                Operation = SproutOperation.Get,
                Data = data,
                Affected = data.Count,
            };
        }

        // Manual paging: page N size M
        if (q.Page.HasValue)
        {
            var pageSize = Math.Min(q.Size ?? defaultPageSize, defaultPageSize);
            return ApplyPaging(query, data, q.Page.Value, pageSize);
        }

        // Auto-paging: if result exceeds default page size
        if (data.Count > defaultPageSize)
        {
            return ApplyPaging(query, data, 1, defaultPageSize);
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = data,
            Affected = data.Count,
        };
    }

    private static SproutResponse ApplyPaging(
        string query, List<Dictionary<string, object?>> data, int page, int pageSize)
    {
        var total = data.Count;
        var skip = (page - 1) * pageSize;
        var pageData = data.Skip(skip).Take(pageSize).ToList();
        var totalPages = (total + pageSize - 1) / pageSize;
        var hasNext = page < totalPages;

        string? nextQuery = null;
        if (hasNext)
        {
            nextQuery = BuildNextPageQuery(query, page + 1, pageSize);
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = pageData,
            Affected = pageData.Count,
            Paging = new PagingInfo
            {
                Total = total,
                PageSize = pageSize,
                Page = page,
                Next = nextQuery,
            },
        };
    }

    private static string BuildNextPageQuery(string query, int nextPage, int pageSize)
    {
        // If query already has 'page N size M', replace it while preserving follow clauses after it.
        // Query syntax order: ... page N size M follow ...
        var pageIdx = query.LastIndexOf(" page ", StringComparison.OrdinalIgnoreCase);
        if (pageIdx >= 0)
        {
            var before = query[..pageIdx];

            // Find the end of "page N size M" — skip past "page", number, "size", number
            var afterPage = pageIdx + 6; // skip " page "
            // Skip page number
            while (afterPage < query.Length && (char.IsDigit(query[afterPage]) || query[afterPage] == ' '))
                afterPage++;
            // Now we might be at "size" or at follow/end
            if (afterPage + 4 < query.Length && query[afterPage..].StartsWith("size", StringComparison.OrdinalIgnoreCase))
            {
                afterPage += 4; // skip "size"
                // Skip spaces and size number
                while (afterPage < query.Length && (char.IsDigit(query[afterPage]) || query[afterPage] == ' '))
                    afterPage++;
            }

            var tail = afterPage < query.Length ? query[afterPage..] : "";
            return $"{before} page {nextPage} size {pageSize}{(tail.Length > 0 ? " " : "")}{tail.TrimStart()}";
        }

        // No existing page clause — insert before follow if present, otherwise append
        var followIdx = query.IndexOf(" follow ", StringComparison.OrdinalIgnoreCase);
        if (followIdx >= 0)
            return $"{query[..followIdx]} page {nextPage} size {pageSize}{query[followIdx..]}";

        return $"{query} page {nextPage} size {pageSize}";
    }

    // ── Aggregate ─────────────────────────────────────────────

    private static SproutResponse ExecuteAggregate(TableHandle table, GetQuery q)
    {
        var filter = WhereEngine.PrepareFilter(table, q.Where);
        var nowMs = table.HasTtl ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;
        var colName = q.AggregateColumn!; // validated non-null before call
        var fn = q.Aggregate!.Value;
        var alias = q.AggregateAlias ?? AggregateName(fn);

        // Read values for the aggregate column, applying where filter
        var values = ReadAggregateValues(table, colName, filter, nowMs);

        object? result = fn switch
        {
            AggregateFunction.Sum => ComputeSum(values),
            AggregateFunction.Avg => ComputeAvg(values),
            AggregateFunction.Min => ComputeMinMax(values, min: true),
            AggregateFunction.Max => ComputeMinMax(values, min: false),
            AggregateFunction.Count => (long)values.Count,
            _ => null,
        };

        var row = new Dictionary<string, object?> { [alias] = result };

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = [row],
            Affected = 1,
        };
    }

    // ── Grouped (group by) ─────────────────────────────────────

    private static SproutResponse ExecuteGrouped(TableHandle table, GetQuery q)
    {
        var filter = WhereEngine.PrepareFilter(table, q.Where);
        var nowMs = table.HasTtl ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;
        var groupByCols = q.GroupBy!; // validated non-null before call

        // Build groups: key = composite of group-by values, value = list of (id, place)
        var groups = BuildGroups(table, groupByCols, filter, nowMs);

        // Build result rows
        var data = new List<Dictionary<string, object?>>(groups.Count);

        if (q.Aggregate.HasValue && q.AggregateColumn is not null)
        {
            // Aggregate per group
            var colName = q.AggregateColumn;
            var fn = q.Aggregate.Value;
            var alias = q.AggregateAlias ?? AggregateName(fn);
            ColumnHandle? aggHandle = colName == "_id" ? null : table.GetColumn(colName);

            foreach (var (groupKey, members) in groups)
            {
                var row = new Dictionary<string, object?>(groupByCols.Count + 1);

                // Add group-by column values from the first member
                var (firstId, firstPlace) = members[0];
                foreach (var col in groupByCols)
                {
                    if (col.Name == "_id")
                        row["_id"] = firstId;
                    else
                        row[col.Name] = table.GetColumn(col.Name).ReadValue(firstPlace);
                }

                // Compute aggregate for this group
                var values = new List<object>(members.Count);
                foreach (var (id, place) in members)
                {
                    object? val = aggHandle is null ? (object)id : aggHandle.ReadValue(place);
                    if (val is not null)
                        values.Add(val);
                }

                row[alias] = fn switch
                {
                    AggregateFunction.Sum => ComputeSum(values),
                    AggregateFunction.Avg => ComputeAvg(values),
                    AggregateFunction.Min => ComputeMinMax(values, min: true),
                    AggregateFunction.Max => ComputeMinMax(values, min: false),
                    AggregateFunction.Count => (long)values.Count,
                    _ => null,
                };

                data.Add(row);
            }
        }
        else if (q.IsCount)
        {
            // Count per group
            foreach (var (groupKey, members) in groups)
            {
                var row = new Dictionary<string, object?>(groupByCols.Count + 1);

                var (firstId, firstPlace) = members[0];
                foreach (var col in groupByCols)
                {
                    if (col.Name == "_id")
                        row["_id"] = firstId;
                    else
                        row[col.Name] = table.GetColumn(col.Name).ReadValue(firstPlace);
                }

                row["count"] = members.Count;
                data.Add(row);
            }
        }
        else
        {
            // group by without aggregate or count — just return distinct groups with count
            foreach (var (groupKey, members) in groups)
            {
                var row = new Dictionary<string, object?>(groupByCols.Count + 1);

                var (firstId, firstPlace) = members[0];
                foreach (var col in groupByCols)
                {
                    if (col.Name == "_id")
                        row["_id"] = firstId;
                    else
                        row[col.Name] = table.GetColumn(col.Name).ReadValue(firstPlace);
                }

                row["count"] = members.Count;
                data.Add(row);
            }
        }

        // Order by (on grouped results)
        if (q.OrderBy is not null && q.OrderBy.Count > 0)
        {
            var orderColumns = q.OrderBy;
            data.Sort((a, b) =>
            {
                foreach (var col in orderColumns)
                {
                    a.TryGetValue(col.Name, out var valA);
                    b.TryGetValue(col.Name, out var valB);
                    var cmp = CompareValues(valA, valB);
                    if (col.Descending) cmp = -cmp;
                    if (cmp != 0) return cmp;
                }
                return 0;
            });
        }

        // Limit
        if (q.Limit.HasValue)
        {
            data = data.Take(q.Limit.Value).ToList();
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = data,
            Affected = data.Count,
        };
    }

    private static Dictionary<string, List<(ulong Id, long Place)>> BuildGroups(
        TableHandle table, List<SelectColumn> groupByCols, WhereEngine.FilterNode? filter, long nowMs = 0)
    {
        var groups = new Dictionary<string, List<(ulong, long)>>();
        var ttl = table.Ttl;

        // Resolve group-by column handles
        var handles = new (string Name, ColumnHandle? Handle)[groupByCols.Count];
        for (var i = 0; i < groupByCols.Count; i++)
        {
            var col = groupByCols[i];
            handles[i] = (col.Name, col.Name == "_id" ? null : table.GetColumn(col.Name));
        }

        table.Index.ForEachUsed((id, place) =>
        {
            if (ttl is not null && nowMs > 0) { var ea = ttl.ReadExpiresAt(place); if (ea > 0 && nowMs > ea) return; }
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) return;

            // Build group key from column values
            var keyParts = new string[handles.Length];
            for (var i = 0; i < handles.Length; i++)
            {
                object? val = handles[i].Handle is null ? (object)id : handles[i].Handle!.ReadValue(place);
                keyParts[i] = val?.ToString() ?? "\0null";
            }
            var key = string.Join("\x1F", keyParts);

            if (!groups.TryGetValue(key, out var list))
            {
                list = [];
                groups[key] = list;
            }
            list.Add((id, place));
        });

        return groups;
    }

    // ── Follow (join) ─────────────────────────────────────────

    private static List<Dictionary<string, object?>> ExecuteFollow(
        List<Dictionary<string, object?>> data,
        FollowClause follow,
        TableHandle targetTable,
        WhereEngine.FilterNode? targetFilter,
        string baseTable)
    {
        // Build a lookup: for each row in the target table, group by the join column value
        var targetIndex = BuildTargetIndex(targetTable, follow.TargetColumn, targetFilter);

        // Resolve target columns for projection (apply follow select if present)
        var targetColumns = new List<(string Name, bool HasAlias, ColumnHandle Handle)>();
        if (follow.Select is not null)
        {
            foreach (var sel in follow.Select)
            {
                if (sel.Name == "_id") continue; // _id is always included separately
                if (targetTable.HasColumn(sel.Name))
                    targetColumns.Add((sel.OutputName, sel.Alias is not null, targetTable.GetColumn(sel.Name)));
            }
        }
        else
        {
            foreach (var col in targetTable.Schema.Columns)
                targetColumns.Add((col.Name, false, targetTable.GetColumn(col.Name)));
        }

        var alias = follow.Alias;
        var result = new List<Dictionary<string, object?>>();

        // Base table columns are stored directly ("_id", "name").
        // Columns from previous follows are stored as "alias.column" ("orders._id").
        // If SourceTable matches the base query table, use direct key. Otherwise aliased.
        var sourceKey = follow.SourceTable == baseTable
            ? follow.SourceColumn
            : $"{follow.SourceTable}.{follow.SourceColumn}";

        var joinType = follow.JoinType;
        var matchedTargetKeys = joinType is JoinType.Right or JoinType.Outer
            ? new HashSet<string>()
            : null;

        foreach (var row in data)
        {
            // Get the source value to join on
            if (!row.TryGetValue(sourceKey, out var sourceValue) || sourceValue is null)
            {
                // Left/Outer: keep source row with null target columns
                if (joinType is JoinType.Left or JoinType.Outer)
                    result.Add(BuildNullTargetRow(row, alias, targetColumns));
                continue;
            }

            // Look up matching target rows
            var key = sourceValue.ToString() ?? "";
            if (targetIndex.TryGetValue(key, out var places))
            {
                matchedTargetKeys?.Add(key);

                // Expand: one output row per matching child
                foreach (var (id, place) in places)
                {
                    var flat = new Dictionary<string, object?>(row);
                    flat[$"{alias}._id"] = id;
                    foreach (var (name, hasAlias, handle) in targetColumns)
                        flat[hasAlias ? name : $"{alias}.{name}"] = handle.ReadValue(place);
                    result.Add(flat);
                }
            }
            else if (joinType is JoinType.Left or JoinType.Outer)
            {
                // Left/Outer: no match → keep source row with null target columns
                result.Add(BuildNullTargetRow(row, alias, targetColumns));
            }
            // Inner/Right: no match → row dropped
        }

        // Right/Outer: add unmatched target rows with null source columns
        if (matchedTargetKeys is not null)
        {
            var sourceColumns = data.Count > 0 ? data[0].Keys.ToList() : [];
            foreach (var (targetKey, places) in targetIndex)
            {
                if (matchedTargetKeys.Contains(targetKey))
                    continue;

                foreach (var (id, place) in places)
                {
                    var flat = new Dictionary<string, object?>();
                    foreach (var col in sourceColumns)
                        flat[col] = null;
                    flat[$"{alias}._id"] = id;
                    foreach (var (name, hasAlias, handle) in targetColumns)
                        flat[hasAlias ? name : $"{alias}.{name}"] = handle.ReadValue(place);
                    result.Add(flat);
                }
            }
        }

        return result;
    }

    private static Dictionary<string, object?> BuildNullTargetRow(
        Dictionary<string, object?> sourceRow, string alias,
        List<(string Name, bool HasAlias, ColumnHandle Handle)> targetColumns)
    {
        var flat = new Dictionary<string, object?>(sourceRow);
        flat[$"{alias}._id"] = null;
        foreach (var (name, hasAlias, _) in targetColumns)
            flat[hasAlias ? name : $"{alias}.{name}"] = null;
        return flat;
    }

    private static Dictionary<string, List<(ulong Id, long Place)>> BuildTargetIndex(
        TableHandle table, string joinColumn, WhereEngine.FilterNode? filter)
    {
        var index = new Dictionary<string, List<(ulong, long)>>();
        ColumnHandle? colHandle = joinColumn == "_id" ? null : table.GetColumn(joinColumn);
        var ttl = table.Ttl;
        var nowMs = ttl is not null ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;

        table.Index.ForEachUsed((id, place) =>
        {
            if (ttl is not null && nowMs > 0) { var ea = ttl.ReadExpiresAt(place); if (ea > 0 && nowMs > ea) return; }
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) return;

            object? val = colHandle is null ? (object)id : colHandle.ReadValue(place);
            if (val is null) return;

            var key = val.ToString() ?? "";
            if (!index.TryGetValue(key, out var list))
            {
                list = [];
                index[key] = list;
            }
            list.Add((id, place));
        });

        return index;
    }

    private static List<object> ReadAggregateValues(TableHandle table, string colName, WhereEngine.FilterNode? filter, long nowMs = 0)
    {
        var values = new List<object>();
        var ttl = table.Ttl;

        if (colName == "_id")
        {
            table.Index.ForEachUsed((id, place) =>
            {
                if (ttl is not null && nowMs > 0) { var ea = ttl.ReadExpiresAt(place); if (ea > 0 && nowMs > ea) return; }
                if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) return;
                values.Add(id);
            });
            return values;
        }

        var handle = table.GetColumn(colName);
        table.Index.ForEachUsed((id, place) =>
        {
            if (ttl is not null && nowMs > 0) { var ea = ttl.ReadExpiresAt(place); if (ea > 0 && nowMs > ea) return; }
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) return;

            var val = handle.ReadValue(place);
            if (val is not null)
                values.Add(val);
        });
        return values;
    }

    private static double ComputeSum(List<object> values)
    {
        var sum = 0.0;
        foreach (var v in values)
            sum += Convert.ToDouble(v);
        return sum;
    }

    private static double? ComputeAvg(List<object> values)
    {
        if (values.Count == 0) return null;
        return ComputeSum(values) / values.Count;
    }

    private static object? ComputeMinMax(List<object> values, bool min)
    {
        if (values.Count == 0) return null;

        var best = values[0];
        for (var i = 1; i < values.Count; i++)
        {
            if (best is IComparable cmp)
            {
                var comparison = cmp.CompareTo(values[i]);
                if (min ? comparison > 0 : comparison < 0)
                    best = values[i];
            }
        }
        return best;
    }

    private static bool IsNumericColumn(TableHandle table, string colName)
    {
        var handle = table.GetColumn(colName);
        return handle.Type is ColumnType.SByte or ColumnType.UByte
            or ColumnType.SShort or ColumnType.UShort
            or ColumnType.SInt or ColumnType.UInt
            or ColumnType.SLong or ColumnType.ULong
            or ColumnType.Float or ColumnType.Double;
    }

    private static string AggregateName(AggregateFunction fn) => fn switch
    {
        AggregateFunction.Sum => "sum",
        AggregateFunction.Avg => "avg",
        AggregateFunction.Min => "min",
        AggregateFunction.Max => "max",
        AggregateFunction.Count => "count",
        _ => fn.ToString().ToLowerInvariant(),
    };

    private static string BuildDistinctKey(Dictionary<string, object?> row)
    {
        // Build a composite key from all column values
        var parts = new string[row.Count];
        var i = 0;
        foreach (var kvp in row)
        {
            parts[i++] = kvp.Value?.ToString() ?? "\0null";
        }
        return string.Join("\x1F", parts);
    }

    private static int CompareValues(object? a, object? b)
    {
        if (a is null && b is null) return 0;
        if (a is null) return -1;
        if (b is null) return 1;

        if (a is IComparable ca)
            return ca.CompareTo(b);

        return string.Compare(a.ToString(), b.ToString(), StringComparison.Ordinal);
    }

    // ── Read rows ─────────────────────────────────────────────

    private static List<Dictionary<string, object?>> ReadRows(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect, WhereEngine.FilterNode? filter, long nowMs = 0)
    {
        var projection = ResolveProjection(table, selectColumns, excludeSelect);
        var data = new List<Dictionary<string, object?>>();
        var ttl = table.Ttl;

        table.Index.ForEachUsed((id, place) =>
        {
            // TTL filter: skip expired rows
            if (ttl is not null && nowMs > 0)
            {
                var expiresAt = ttl.ReadExpiresAt(place);
                if (expiresAt > 0 && nowMs > expiresAt) return;
            }

            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place))
                return;
            data.Add(ProjectRow(projection, id, place));
        });

        return data;
    }

    /// <summary>
    /// A projection entry: either _id (Handle is null) or a real column.
    /// Preserves the order from the SELECT clause.
    /// </summary>
    private readonly record struct ProjectionEntry(string Name, ColumnHandle? Handle);

    /// <summary>
    /// Builds an ordered projection list that respects the SELECT column order.
    /// _id is included at the position specified in SELECT (or first if no SELECT).
    /// </summary>
    private static List<ProjectionEntry> ResolveProjection(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect)
    {
        if (selectColumns is null)
        {
            // All columns: _id first, then table columns
            var all = new List<ProjectionEntry>(table.Schema.Columns.Count + 1)
            {
                new("_id", null)
            };
            foreach (var col in table.Schema.Columns)
                all.Add(new ProjectionEntry(col.Name, table.GetColumn(col.Name)));
            return all;
        }

        if (excludeSelect)
        {
            var excluded = new HashSet<string>(selectColumns.Count);
            foreach (var col in selectColumns)
                excluded.Add(col.Name);

            var result = new List<ProjectionEntry>(table.Schema.Columns.Count + 1);
            if (!excluded.Contains("_id"))
                result.Add(new ProjectionEntry("_id", null));
            foreach (var col in table.Schema.Columns)
            {
                if (!excluded.Contains(col.Name))
                    result.Add(new ProjectionEntry(col.Name, table.GetColumn(col.Name)));
            }
            return result;
        }

        {
            // Preserve SELECT order — _id at the position where it appears in the list
            var result = new List<ProjectionEntry>(selectColumns.Count);
            foreach (var col in selectColumns)
            {
                if (col.Name == "_id")
                    result.Add(new ProjectionEntry(col.OutputName, null));
                else
                    result.Add(new ProjectionEntry(col.OutputName, table.GetColumn(col.Name)));
            }
            return result;
        }
    }

    private static Dictionary<string, object?> ProjectRow(
        List<ProjectionEntry> projection, ulong id, long place)
    {
        var record = new Dictionary<string, object?>(projection.Count);
        foreach (var entry in projection)
        {
            if (entry.Handle is null)
                record[entry.Name] = id;
            else
                record[entry.Name] = entry.Handle.ReadValue(place);
        }
        return record;
    }

    // ── Computed fields ────────────────────────────────────────

    /// <summary>
    /// Finds columns referenced by computed fields that are not already in the select list.
    /// Returns null if no extra columns are needed.
    /// </summary>
    private static List<SelectColumn>? ResolveExtraColumnsForComputed(GetQuery q)
    {
        if (q.ComputedSelect is null)
            return null;

        var selectedNames = new HashSet<string>();
        if (q.Select is not null)
        {
            foreach (var col in q.Select)
                selectedNames.Add(col.Name);
        }

        List<SelectColumn>? extras = null;

        foreach (var comp in q.ComputedSelect)
        {
            if (!selectedNames.Contains(comp.LeftColumn) && comp.LeftColumn != "_id")
            {
                extras ??= [];
                if (!extras.Exists(e => e.Name == comp.LeftColumn))
                    extras.Add(new SelectColumn(comp.LeftColumn, comp.LeftPosition, comp.LeftLength));
            }

            if (comp.RightColumn is not null && !selectedNames.Contains(comp.RightColumn) && comp.RightColumn != "_id")
            {
                extras ??= [];
                if (!extras.Exists(e => e.Name == comp.RightColumn))
                    extras.Add(new SelectColumn(comp.RightColumn, comp.RightPosition, comp.RightLength));
            }
        }

        return extras;
    }

    private static List<SelectColumn>? ResolveExtraColumnsForFollow(GetQuery q)
    {
        if (q.Follow is null || q.Select is null)
            return null;

        var selectedNames = new HashSet<string>(q.Select.Count);
        foreach (var col in q.Select)
            selectedNames.Add(col.Name);

        List<SelectColumn>? extras = null;

        foreach (var follow in q.Follow)
        {
            if (!selectedNames.Contains(follow.SourceColumn))
            {
                extras ??= [];
                if (!extras.Exists(e => e.Name == follow.SourceColumn))
                    extras.Add(new SelectColumn(follow.SourceColumn, 0, 0));
            }
        }

        return extras;
    }

    private static List<SelectColumn>? MergeExtras(List<SelectColumn>? a, List<SelectColumn>? b)
    {
        if (a is null) return b;
        if (b is null) return a;
        var merged = new List<SelectColumn>(a.Count + b.Count);
        merged.AddRange(a);
        foreach (var item in b)
        {
            if (!merged.Exists(e => e.Name == item.Name))
                merged.Add(item);
        }
        return merged;
    }

    private static List<SelectColumn>? MergeSelectWithExtra(List<SelectColumn>? select, List<SelectColumn>? extras)
    {
        if (extras is null)
            return select;

        var merged = new List<SelectColumn>((select?.Count ?? 0) + extras.Count);
        if (select is not null)
            merged.AddRange(select);
        merged.AddRange(extras);
        return merged;
    }

    private static void ApplyComputedColumns(
        List<Dictionary<string, object?>> data,
        List<ComputedColumn> computedColumns,
        TableHandle table)
    {
        foreach (var row in data)
        {
            foreach (var comp in computedColumns)
            {
                row.TryGetValue(comp.LeftColumn, out var leftVal);

                object? rightVal;
                if (comp.RightColumn is not null)
                {
                    row.TryGetValue(comp.RightColumn, out rightVal);
                }
                else
                {
                    rightVal = comp.RightLiteral;
                }

                row[comp.Alias] = ComputeArithmetic(leftVal, rightVal, comp.Operator);
            }
        }
    }

    private static object? ComputeArithmetic(object? left, object? right, ArithmeticOp op)
    {
        if (left is null || right is null)
            return null;

        // If either is float/double, or division → use double
        if (left is float or double || right is float or double || op == ArithmeticOp.Divide)
        {
            var l = Convert.ToDouble(left);
            var r = Convert.ToDouble(right);
            return op switch
            {
                ArithmeticOp.Add => l + r,
                ArithmeticOp.Subtract => l - r,
                ArithmeticOp.Multiply => l * r,
                ArithmeticOp.Divide => r != 0 ? l / r : null,
                _ => null,
            };
        }

        // Integer arithmetic — use signed long if any operand is signed, otherwise ulong
        var isSigned = left is sbyte or short or int or long
                    || right is sbyte or short or int or long;

        if (isSigned)
        {
            var l = Convert.ToInt64(left);
            var r = Convert.ToInt64(right);
            return op switch
            {
                ArithmeticOp.Add => l + r,
                ArithmeticOp.Subtract => l - r,
                ArithmeticOp.Multiply => l * r,
                _ => null,
            };
        }
        else
        {
            var l = Convert.ToUInt64(left);
            var r = Convert.ToUInt64(right);
            return op switch
            {
                ArithmeticOp.Add => l + r,
                ArithmeticOp.Subtract => l - r,
                ArithmeticOp.Multiply => l * r,
                _ => null,
            };
        }
    }

    // ── B-Tree shortcut ──────────────────────────────────────

    /// <summary>
    /// Checks if the WHERE clause is a simple top-level comparison on a B-Tree-indexed column.
    /// Returns a list of candidate places from the B-Tree, or null if no shortcut is possible.
    /// </summary>
    private static List<long>? TryBTreeLookup(TableHandle table, WhereNode? where, WhereEngine.FilterNode? filter)
    {
        if (where is not CompareNode compare)
            return null;

        if (compare.Column == "_id")
            return null;

        if (!table.HasBTree(compare.Column))
            return null;

        if (WhereEngine.IsStringOp(compare.Operator))
            return null;

        var btree = table.GetBTree(compare.Column);
        var colHandle = table.GetColumn(compare.Column);

        switch (compare.Operator)
        {
            case CompareOp.Equal:
            {
                var encoded = colHandle.EncodeValueToBytes(compare.Value);
                return btree.Lookup(encoded);
            }

            case CompareOp.GreaterThan:
            case CompareOp.GreaterThanOrEqual:
            {
                var encoded = colHandle.EncodeValueToBytes(compare.Value);
                return btree.RangeLookup(encoded, null);
            }

            case CompareOp.LessThan:
            case CompareOp.LessThanOrEqual:
            {
                var encoded = colHandle.EncodeValueToBytes(compare.Value);
                return btree.RangeLookup(null, encoded);
            }

            case CompareOp.Between:
            {
                if (compare.Value2 is null) return null;
                var low = colHandle.EncodeValueToBytes(compare.Value);
                var high = colHandle.EncodeValueToBytes(compare.Value2);
                return btree.RangeLookup(low, high);
            }

            default:
                return null;
        }
    }

    /// <summary>
    /// Reads rows from a pre-computed list of places (from B-Tree lookup).
    /// Still applies the full filter for operators like GT/LT that return inclusive ranges.
    /// </summary>
    private static List<Dictionary<string, object?>> ReadRowsByPlaces(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect,
        List<long> places, WhereEngine.FilterNode? filter, long nowMs = 0)
    {
        var projection = ResolveProjection(table, selectColumns, excludeSelect);
        var data = new List<Dictionary<string, object?>>(places.Count);
        var ttl = table.Ttl;

        foreach (var place in places)
        {
            // Resolve ID for this place
            var id = table.Index.FindIdForPlace(place);
            if (id == 0) continue; // place no longer valid (deleted)

            // TTL filter: skip expired rows
            if (ttl is not null && nowMs > 0)
            {
                var expiresAt = ttl.ReadExpiresAt(place);
                if (expiresAt > 0 && nowMs > expiresAt) continue;
            }

            // Apply the full filter to handle GT vs GTE, etc.
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place))
                continue;

            data.Add(ProjectRow(projection, id, place));
        }

        return data;
    }
}

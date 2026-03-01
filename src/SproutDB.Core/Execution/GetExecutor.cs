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
                if (col.Name != "id" && !table.HasColumn(col.Name))
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
                if (comp.LeftColumn != "id" && !table.HasColumn(comp.LeftColumn))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{comp.LeftColumn}' does not exist", Position = comp.LeftPosition, Length = comp.LeftLength });
                }
                if (comp.RightColumn is not null && comp.RightColumn != "id" && !table.HasColumn(comp.RightColumn))
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
                if (col.Name != "id" && !table.HasColumn(col.Name)
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
            if (q.AggregateColumn != "id" && !table.HasColumn(q.AggregateColumn))
            {
                validationErrors ??= [];
                validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{q.AggregateColumn}' does not exist", Position = q.AggregateColumnPosition, Length = q.AggregateColumnLength });
            }
            else if (q.Aggregate is AggregateFunction.Sum or AggregateFunction.Avg
                     && q.AggregateColumn != "id" && !IsNumericColumn(table, q.AggregateColumn))
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
                if (follow.SourceColumn != "id" && (sourceTable is null || !sourceTable.HasColumn(follow.SourceColumn)))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{follow.SourceColumn}' does not exist on '{follow.SourceTable}'", Position = follow.SourceColumnPosition, Length = follow.SourceColumnLength });
                }

                // Validate target column exists
                if (follow.TargetColumn != "id" && !targetTable.HasColumn(follow.TargetColumn))
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
                if (col.Name != "id" && !table.HasColumn(col.Name))
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

        // If computed fields reference columns not in select, include them temporarily
        var extraColumns = ResolveExtraColumnsForComputed(q);
        var effectiveSelect = MergeSelectWithExtra(q.Select, extraColumns);

        // Try B-Tree shortcut for simple WHERE conditions
        var btreeResult = TryBTreeLookup(table, q.Where, filter);
        var data = btreeResult is not null
            ? ReadRowsByPlaces(table, effectiveSelect, q.ExcludeSelect, btreeResult, filter)
            : ReadRows(table, effectiveSelect, q.ExcludeSelect, filter).ToList();

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

        // Follow (join): attach nested rows from target tables
        if (q.Follow is not null && tableResolver is not null)
        {
            foreach (var follow in q.Follow)
            {
                var targetTable = tableResolver(follow.TargetTable);
                if (targetTable is null) continue;

                var targetFilter = WhereEngine.PrepareFilter(targetTable, follow.Where);
                ExecuteFollow(data, follow, targetTable, targetFilter);
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
        // If query already has 'page N size M', replace it
        // Otherwise append it
        var pageIdx = query.LastIndexOf(" page ", StringComparison.OrdinalIgnoreCase);
        if (pageIdx >= 0)
        {
            return $"{query[..pageIdx]} page {nextPage} size {pageSize}";
        }

        return $"{query} page {nextPage} size {pageSize}";
    }

    // ── Aggregate ─────────────────────────────────────────────

    private static SproutResponse ExecuteAggregate(TableHandle table, GetQuery q)
    {
        var filter = WhereEngine.PrepareFilter(table, q.Where);
        var colName = q.AggregateColumn!; // validated non-null before call
        var fn = q.Aggregate!.Value;
        var alias = q.AggregateAlias ?? AggregateName(fn);

        // Read values for the aggregate column, applying where filter
        var values = ReadAggregateValues(table, colName, filter);

        object? result = fn switch
        {
            AggregateFunction.Sum => ComputeSum(values),
            AggregateFunction.Avg => ComputeAvg(values),
            AggregateFunction.Min => ComputeMinMax(values, min: true),
            AggregateFunction.Max => ComputeMinMax(values, min: false),
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
        var groupByCols = q.GroupBy!; // validated non-null before call

        // Build groups: key = composite of group-by values, value = list of (id, place)
        var groups = BuildGroups(table, groupByCols, filter);

        // Build result rows
        var data = new List<Dictionary<string, object?>>(groups.Count);

        if (q.Aggregate.HasValue && q.AggregateColumn is not null)
        {
            // Aggregate per group
            var colName = q.AggregateColumn;
            var fn = q.Aggregate.Value;
            var alias = q.AggregateAlias ?? AggregateName(fn);
            ColumnHandle? aggHandle = colName == "id" ? null : table.GetColumn(colName);

            foreach (var (groupKey, members) in groups)
            {
                var row = new Dictionary<string, object?>(groupByCols.Count + 1);

                // Add group-by column values from the first member
                var (firstId, firstPlace) = members[0];
                foreach (var col in groupByCols)
                {
                    if (col.Name == "id")
                        row["id"] = firstId;
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
                    if (col.Name == "id")
                        row["id"] = firstId;
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
                    if (col.Name == "id")
                        row["id"] = firstId;
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
        TableHandle table, List<SelectColumn> groupByCols, WhereEngine.FilterNode? filter)
    {
        var groups = new Dictionary<string, List<(ulong, long)>>();
        var nextId = table.Index.ReadNextId();

        // Resolve group-by column handles
        var handles = new (string Name, ColumnHandle? Handle)[groupByCols.Count];
        for (var i = 0; i < groupByCols.Count; i++)
        {
            var col = groupByCols[i];
            handles[i] = (col.Name, col.Name == "id" ? null : table.GetColumn(col.Name));
        }

        for (ulong id = 1; id < nextId; id++)
        {
            var place = table.Index.ReadPlace(id);
            if (place < 0) continue;
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) continue;

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
        }

        return groups;
    }

    // ── Follow (join) ─────────────────────────────────────────

    private static void ExecuteFollow(
        List<Dictionary<string, object?>> data,
        FollowClause follow,
        TableHandle targetTable,
        WhereEngine.FilterNode? targetFilter)
    {
        // Build a lookup: for each row in the target table, group by the join column value
        var targetIndex = BuildTargetIndex(targetTable, follow.TargetColumn, targetFilter);

        // Resolve all target columns for projection
        var targetColumns = new List<(string Name, ColumnHandle Handle)>(targetTable.Schema.Columns.Count);
        foreach (var col in targetTable.Schema.Columns)
            targetColumns.Add((col.Name, targetTable.GetColumn(col.Name)));

        foreach (var row in data)
        {
            // Get the source value to join on
            if (!row.TryGetValue(follow.SourceColumn, out var sourceValue) || sourceValue is null)
            {
                row[follow.Alias] = Array.Empty<Dictionary<string, object?>>();
                continue;
            }

            // Look up matching target rows
            var key = sourceValue.ToString() ?? "";
            if (targetIndex.TryGetValue(key, out var places))
            {
                var nested = new List<Dictionary<string, object?>>(places.Count);
                foreach (var (id, place) in places)
                {
                    var record = new Dictionary<string, object?>(targetColumns.Count + 1)
                    {
                        ["id"] = id
                    };
                    foreach (var (name, handle) in targetColumns)
                        record[name] = handle.ReadValue(place);
                    nested.Add(record);
                }
                row[follow.Alias] = nested;
            }
            else
            {
                row[follow.Alias] = Array.Empty<Dictionary<string, object?>>();
            }
        }
    }

    private static Dictionary<string, List<(ulong Id, long Place)>> BuildTargetIndex(
        TableHandle table, string joinColumn, WhereEngine.FilterNode? filter)
    {
        var index = new Dictionary<string, List<(ulong, long)>>();
        var nextId = table.Index.ReadNextId();

        ColumnHandle? colHandle = joinColumn == "id" ? null : table.GetColumn(joinColumn);

        for (ulong id = 1; id < nextId; id++)
        {
            var place = table.Index.ReadPlace(id);
            if (place < 0) continue;

            // Apply follow where filter
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) continue;

            // Get the join column value
            object? val = colHandle is null ? (object)id : colHandle.ReadValue(place);
            if (val is null) continue;

            var key = val.ToString() ?? "";
            if (!index.TryGetValue(key, out var list))
            {
                list = [];
                index[key] = list;
            }
            list.Add((id, place));
        }

        return index;
    }

    private static List<object> ReadAggregateValues(TableHandle table, string colName, WhereEngine.FilterNode? filter)
    {
        var values = new List<object>();
        var nextId = table.Index.ReadNextId();

        // For "id" column, the value is the id itself
        if (colName == "id")
        {
            for (ulong id = 1; id < nextId; id++)
            {
                var place = table.Index.ReadPlace(id);
                if (place < 0) continue;
                if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) continue;
                values.Add(id);
            }
            return values;
        }

        var handle = table.GetColumn(colName);
        for (ulong id = 1; id < nextId; id++)
        {
            var place = table.Index.ReadPlace(id);
            if (place < 0) continue;
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) continue;

            var val = handle.ReadValue(place);
            if (val is not null)
                values.Add(val);
        }
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

    private static IEnumerable<Dictionary<string, object?>> ReadRows(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect, WhereEngine.FilterNode? filter)
    {
        // Determine which columns to project
        bool includeId;
        if (excludeSelect)
            includeId = selectColumns is null || !selectColumns.Exists(c => c.Name == "id");
        else
            includeId = selectColumns is null || selectColumns.Exists(c => c.Name == "id");

        var columns = ResolveColumns(table, selectColumns, excludeSelect);

        var nextId = table.Index.ReadNextId();

        for (ulong id = 1; id < nextId; id++)
        {
            var place = table.Index.ReadPlace(id);
            if (place < 0)
                continue; // deleted / free slot

            // Apply where filter
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place))
                continue;

            var record = new Dictionary<string, object?>(columns.Count + 1);

            if (includeId)
                record["id"] = id;

            foreach (var (name, handle) in columns)
                record[name] = handle.ReadValue(place);

            yield return record;
        }
    }

    private static List<(string Name, ColumnHandle Handle)> ResolveColumns(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect)
    {
        if (selectColumns is null)
        {
            // All columns
            var all = new List<(string, ColumnHandle)>(table.Schema.Columns.Count);
            foreach (var col in table.Schema.Columns)
                all.Add((col.Name, table.GetColumn(col.Name)));
            return all;
        }

        if (excludeSelect)
        {
            // All columns EXCEPT the named ones
            var excluded = new HashSet<string>(selectColumns.Count);
            foreach (var col in selectColumns)
                excluded.Add(col.Name);

            var result = new List<(string, ColumnHandle)>(table.Schema.Columns.Count);
            foreach (var col in table.Schema.Columns)
            {
                if (!excluded.Contains(col.Name))
                    result.Add((col.Name, table.GetColumn(col.Name)));
            }
            return result;
        }

        {
            // Only selected columns (excluding "id" which is handled separately)
            var result = new List<(string, ColumnHandle)>(selectColumns.Count);
            foreach (var col in selectColumns)
            {
                if (col.Name == "id") continue;
                result.Add((col.Name, table.GetColumn(col.Name)));
            }
            return result;
        }
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
            if (!selectedNames.Contains(comp.LeftColumn) && comp.LeftColumn != "id")
            {
                extras ??= [];
                if (!extras.Exists(e => e.Name == comp.LeftColumn))
                    extras.Add(new SelectColumn(comp.LeftColumn, comp.LeftPosition, comp.LeftLength));
            }

            if (comp.RightColumn is not null && !selectedNames.Contains(comp.RightColumn) && comp.RightColumn != "id")
            {
                extras ??= [];
                if (!extras.Exists(e => e.Name == comp.RightColumn))
                    extras.Add(new SelectColumn(comp.RightColumn, comp.RightPosition, comp.RightLength));
            }
        }

        return extras;
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

        if (compare.Column == "id")
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
        List<long> places, WhereEngine.FilterNode? filter)
    {
        bool includeId;
        if (excludeSelect)
            includeId = selectColumns is null || !selectColumns.Exists(c => c.Name == "id");
        else
            includeId = selectColumns is null || selectColumns.Exists(c => c.Name == "id");

        var columns = ResolveColumns(table, selectColumns, excludeSelect);
        var data = new List<Dictionary<string, object?>>(places.Count);

        foreach (var place in places)
        {
            // Resolve ID for this place
            var id = table.Index.FindIdForPlace(place);
            if (id == 0) continue; // place no longer valid (deleted)

            // Apply the full filter to handle GT vs GTE, etc.
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place))
                continue;

            var record = new Dictionary<string, object?>(columns.Count + 1);

            if (includeId)
                record["id"] = id;

            foreach (var (name, handle) in columns)
                record[name] = handle.ReadValue(place);

            data.Add(record);
        }

        return data;
    }
}

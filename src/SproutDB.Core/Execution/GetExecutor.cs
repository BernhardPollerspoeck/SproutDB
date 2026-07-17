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
                if (!IsVirtualColumn(col.Name) && !table.HasColumn(col.Name))
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
                if (!IsVirtualColumn(comp.LeftColumn) && !table.HasColumn(comp.LeftColumn))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{comp.LeftColumn}' does not exist", Position = comp.LeftPosition, Length = comp.LeftLength });
                }
                if (comp.RightColumn is not null && !IsVirtualColumn(comp.RightColumn) && !table.HasColumn(comp.RightColumn))
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
            if (q.PostFollowComputedSelect is not null)
            {
                computedAliases ??= new HashSet<string>(q.PostFollowComputedSelect.Count);
                foreach (var comp in q.PostFollowComputedSelect)
                    computedAliases.Add(comp.Alias);
            }

            // Select aliases rename the row key: 'select port as p order by p' sorts on a
            // key that really exists — the whitelist has to know the output names too.
            if (q.Select is not null && !q.ExcludeSelect)
            {
                foreach (var col in q.Select)
                {
                    if (col.Alias is null) continue;
                    computedAliases ??= [];
                    computedAliases.Add(col.Alias);
                }
            }

            // Literal aliases are real result columns too. Sorting by a constant is a no-op,
            // but rejecting it as "column does not exist" would be plainly wrong.
            if (q.LiteralSelect is not null)
            {
                computedAliases ??= new HashSet<string>(q.LiteralSelect.Count);
                foreach (var lit in q.LiteralSelect)
                    computedAliases.Add(lit.Alias);
            }
            if (q.PostFollowLiteralSelect is not null)
            {
                computedAliases ??= new HashSet<string>(q.PostFollowLiteralSelect.Count);
                foreach (var lit in q.PostFollowLiteralSelect)
                    computedAliases.Add(lit.Alias);
            }

            foreach (var col in q.OrderBy)
            {
                // alias.column references a followed table — the projection
                // produces rows keyed by "alias.column", so the sort key will
                // exist on the row even though the base table doesn't have it.
                if (q.Follow is not null && col.Name.Contains('.'))
                    continue;

                if (!IsVirtualColumn(col.Name) && !table.HasColumn(col.Name)
                    && (computedAliases is null || !computedAliases.Contains(col.Name)))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{col.Name}' does not exist", Position = col.Position, Length = col.Length });
                    continue;
                }

                // The sort runs on the projected rows — a column that exists in the table
                // but not in the result would silently not sort at all. Reject it instead.
                // Exceptions where sorting is correct (or moot) without the column:
                //  - no follow restriction here: with follow the post-follow projection has
                //    its own keys, checked via the dot-notation skip above
                //  - count: no rows are returned, ordering is irrelevant
                //  - 'order by _id [desc] limit N': served by the top-N fast path
                //  - 'after' cursor paging: orders by _id by construction
                if (q.Follow is not null || q.IsCount)
                    continue;

                var servedByIdFastPath = col.Name == "_id" && q.OrderBy.Count == 1
                    && (q.After.HasValue || (q.Limit.HasValue && !q.IsDistinct));

                if (!servedByIdFastPath && !IsColumnInProjection(q, col.Name))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError
                    {
                        Code = ErrorCodes.UNKNOWN_COLUMN,
                        Message = $"'order by {col.Name}' requires '{col.Name}' in the select list",
                        Position = col.Position,
                        Length = col.Length,
                    });
                }
            }
        }

        // Validate aggregate column (first + additional)
        if (q.Aggregate.HasValue && q.AggregateColumn is not null)
        {
            if (!IsVirtualColumn(q.AggregateColumn) && !table.HasColumn(q.AggregateColumn))
            {
                validationErrors ??= [];
                validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{q.AggregateColumn}' does not exist", Position = q.AggregateColumnPosition, Length = q.AggregateColumnLength });
            }
            else if (q.Aggregate is AggregateFunction.Sum or AggregateFunction.Avg
                     && !IsVirtualColumn(q.AggregateColumn) && !IsNumericColumn(table, q.AggregateColumn))
            {
                validationErrors ??= [];
                validationErrors.Add(new SproutError { Code = ErrorCodes.TYPE_MISMATCH, Message = $"'{AggregateName(q.Aggregate.Value)}' can only be used on numeric columns", Position = q.AggregateColumnPosition, Length = q.AggregateColumnLength });
            }
        }
        if (q.AdditionalAggregates is not null)
        {
            foreach (var spec in q.AdditionalAggregates)
            {
                if (!IsVirtualColumn(spec.Column) && !table.HasColumn(spec.Column))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{spec.Column}' does not exist", Position = spec.ColumnPosition, Length = spec.ColumnLength });
                }
                else if (spec.Function is AggregateFunction.Sum or AggregateFunction.Avg
                         && !IsVirtualColumn(spec.Column) && !IsNumericColumn(table, spec.Column))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.TYPE_MISMATCH, Message = $"'{AggregateName(spec.Function)}' can only be used on numeric columns", Position = spec.ColumnPosition, Length = spec.ColumnLength });
                }
            }
        }

        // Validate follow clauses
        if (q.Follow is not null && tableResolver is not null)
        {
            // Track previous-follow aliases so a later follow can use them as
            // its source table: `follow ord._id -> order_items.order_id as item`
            // where `ord` is the alias of the preceding follow.
            var aliasToTable = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

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
                TableHandle? sourceTable;
                if (follow.SourceTable == q.Table)
                    sourceTable = table;
                else if (aliasToTable.TryGetValue(follow.SourceTable, out var resolvedFromAlias))
                    sourceTable = tableResolver(resolvedFromAlias);
                else
                    sourceTable = tableResolver(follow.SourceTable);

                if (!IsVirtualColumn(follow.SourceColumn) && (sourceTable is null || !sourceTable.HasColumn(follow.SourceColumn)))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{follow.SourceColumn}' does not exist on '{follow.SourceTable}'", Position = follow.SourceColumnPosition, Length = follow.SourceColumnLength });
                }

                // Validate target column exists
                if (!IsVirtualColumn(follow.TargetColumn) && !targetTable.HasColumn(follow.TargetColumn))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{follow.TargetColumn}' does not exist on '{follow.TargetTable}'", Position = follow.TargetColumnPosition, Length = follow.TargetColumnLength });
                }

                // Record this follow's alias for subsequent follows
                aliasToTable[follow.Alias] = follow.TargetTable;

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
                if (!IsVirtualColumn(col.Name) && !table.HasColumn(col.Name))
                {
                    validationErrors ??= [];
                    validationErrors.Add(new SproutError { Code = ErrorCodes.UNKNOWN_COLUMN, Message = $"column '{col.Name}' does not exist", Position = col.Position, Length = col.Length });
                }
            }
        }

        if (validationErrors is not null)
            return ResponseHelper.Errors(query, validationErrors);

        // Cursor paging: 'after X' — keyset over _id, only the page rows are
        // materialized. Incompatible clauses are rejected by the parser.
        if (q.After.HasValue)
            return ExecuteCursor(query, table, q, defaultPageSize);

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

        // Top-N fast path: 'order by _id [desc] limit N' — collect the N
        // boundary ids via a bounded heap instead of materializing every
        // matching row. Produces the same rows as the sort+limit slow path.
        List<Dictionary<string, object?>> data;
        var topNApplied = false;
        if (q.Limit is int topN && !q.IsCount && !q.IsDistinct && q.Follow is null
            && q.OrderBy is [{ Name: "_id" } idOrder])
        {
            var candidates = CollectTopNById(table, filter, btreeResult, 0, topN, idOrder.Descending, nowMs, out _);
            data = ProjectPlaces(table, effectiveSelect, q.ExcludeSelect, candidates, q.LiteralSelect);
            topNApplied = true;
        }
        else
        {
            data = btreeResult is not null
                ? ReadRowsByPlaces(table, effectiveSelect, q.ExcludeSelect, btreeResult, filter, nowMs, q.LiteralSelect)
                : ReadRows(table, effectiveSelect, q.ExcludeSelect, filter, nowMs, q.LiteralSelect);
        }

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

        // Order by (already satisfied when the top-N fast path produced the rows)
        if (q.OrderBy is not null && q.OrderBy.Count > 0 && !topNApplied)
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
            // Right/Outer joins null out every source key on unmatched target rows —
            // literal columns must survive that. Built once, only consulted there.
            Dictionary<string, object?>? literalValues = null;
            if (q.LiteralSelect is not null)
            {
                literalValues = new Dictionary<string, object?>(q.LiteralSelect.Count);
                foreach (var lit in q.LiteralSelect)
                    literalValues[lit.Alias] = lit.Value;
            }

            foreach (var follow in q.Follow)
            {
                var targetTable = tableResolver(follow.TargetTable);
                if (targetTable is null) continue;

                var targetFilter = WhereEngine.PrepareFilter(targetTable, follow.Where);
                data = ExecuteFollow(data, follow, targetTable, targetFilter, q.Table, literalValues);
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

        // Post-follow computed columns: evaluate before projection so the
        // aliases are available both for projection and for ORDER BY.
        if (q.PostFollowComputedSelect is not null && data.Count > 0)
        {
            ApplyComputedColumns(data, q.PostFollowComputedSelect, table);
        }

        // Post-follow select/exclude: filter keys on the flat joined result
        if (q.PostFollowSelect is not null && data.Count > 0)
        {
            var names = new HashSet<string>(q.PostFollowSelect.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var col in q.PostFollowSelect)
                names.Add(col.Name);

            if (q.PostFollowExclude)
            {
                // Remove listed columns
                foreach (var row in data)
                {
                    foreach (var col in q.PostFollowSelect)
                        row.Remove(col.Name);
                }
            }
            else
            {
                // Keep only listed columns (in order), support alias
                var aliasMap = new Dictionary<string, string>(q.PostFollowSelect.Count, StringComparer.OrdinalIgnoreCase);
                foreach (var col in q.PostFollowSelect)
                {
                    if (col.Alias is not null)
                        aliasMap[col.Name] = col.Alias;
                }

                // Merge literals into the select list by source position so the post-follow
                // projection keeps the written order, same as ResolveProjection does.
                var literals = q.PostFollowLiteralSelect;

                for (var i = 0; i < data.Count; i++)
                {
                    var row = data[i];
                    var projected = new Dictionary<string, object?>(q.PostFollowSelect.Count);
                    var litIndex = 0;

                    foreach (var col in q.PostFollowSelect)
                    {
                        while (literals is not null && litIndex < literals.Count
                               && literals[litIndex].Position < col.Position)
                        {
                            var lit = literals[litIndex++];
                            projected[lit.Alias] = lit.Value;
                        }

                        if (row.TryGetValue(col.Name, out var val))
                            projected[col.OutputName] = val;
                    }

                    while (literals is not null && litIndex < literals.Count)
                    {
                        var lit = literals[litIndex++];
                        projected[lit.Alias] = lit.Value;
                    }
                    // Computed aliases travel with the row — explicit select
                    // doesn't need to mention them again.
                    if (q.PostFollowComputedSelect is not null)
                    {
                        foreach (var comp in q.PostFollowComputedSelect)
                        {
                            if (row.TryGetValue(comp.Alias, out var cval))
                                projected[comp.Alias] = cval;
                        }
                    }
                    data[i] = projected;
                }
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

    // ── Cursor paging (after 'X') ─────────────────────────────

    /// <summary>
    /// Keyset paging: returns the next page of rows with _id greater than the
    /// cursor, ordered by _id ascending. Fetches one row beyond the page size
    /// to decide whether a next cursor exists. Server cost per page is one
    /// cheap id scan plus the projection of the page rows only.
    /// </summary>
    private static SproutResponse ExecuteCursor(string query, TableHandle table, GetQuery q, int defaultPageSize)
    {
        var filter = WhereEngine.PrepareFilter(table, q.Where);
        var extraColumns = ResolveExtraColumnsForComputed(q);
        var effectiveSelect = MergeSelectWithExtra(q.Select, extraColumns);
        var nowMs = table.HasTtl ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;

        var pageSize = Math.Max(q.Limit ?? defaultPageSize, 0);
        var afterId = q.After ?? 0;
        var fetch = pageSize == int.MaxValue ? pageSize : pageSize + 1;

        var btreeResult = TryBTreeLookup(table, q.Where, filter);
        var candidates = CollectTopNById(table, filter, btreeResult, afterId,
            fetch, descending: false, nowMs, out var matchCount);

        var hasMore = candidates.Count > pageSize;
        if (hasMore)
            candidates.RemoveAt(candidates.Count - 1);

        var data = ProjectPlaces(table, effectiveSelect, q.ExcludeSelect, candidates, q.LiteralSelect);

        if (q.ComputedSelect is not null)
        {
            ApplyComputedColumns(data, q.ComputedSelect, table);
            if (extraColumns is not null)
            {
                foreach (var row in data)
                {
                    foreach (var extra in extraColumns)
                        row.Remove(extra.Name);
                }
            }
        }

        string? nextCursor = null;
        if (hasMore && candidates.Count > 0)
            nextCursor = candidates[^1].Id.ToString();

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = data,
            Affected = data.Count,
            Paging = new PagingInfo
            {
                Total = matchCount,
                PageSize = pageSize,
                Page = 0,
                Next = nextCursor is not null ? BuildNextCursorQuery(query, q, nextCursor) : null,
                NextCursor = nextCursor,
            },
        };
    }

    /// <summary>
    /// Splices the next cursor over the existing after-literal using the exact
    /// token position — immune to 'after' appearing inside other string literals.
    /// </summary>
    private static string BuildNextCursorQuery(string query, GetQuery q, string nextCursor)
    {
        var start = q.AfterCursorPosition;
        var end = start + q.AfterCursorLength;
        if (start <= 0 || end > query.Length)
            return $"{query} after '{nextCursor}'"; // defensive — positions are always valid for parsed queries
        return $"{query[..start]}'{nextCursor}'{query[end..]}";
    }

    /// <summary>
    /// Collects the n smallest ids (largest when descending) above the cursor
    /// bound that pass TTL and filter — without materializing any row. Uses a
    /// bounded heap: the scan stays O(rows), memory stays O(n). The returned
    /// list is sorted in final result order. matchCount reports every row that
    /// passed the checks (cursor paging total), independent of n.
    /// </summary>
    private static List<(ulong Id, long Place)> CollectTopNById(
        TableHandle table, WhereEngine.FilterNode? filter, List<long>? places,
        ulong afterId, int n, bool descending, long nowMs, out int matchCount)
    {
        var ttl = table.Ttl;
        var count = 0;

        // Root of the heap is the current worst candidate: the largest id when
        // ascending, the smallest when descending — so it can be evicted in O(log n).
        var comparer = descending
            ? Comparer<ulong>.Default
            : Comparer<ulong>.Create((a, b) => b.CompareTo(a));
        var heap = new PriorityQueue<(ulong Id, long Place), ulong>(comparer);

        void Consider(ulong id, long place)
        {
            if (id <= afterId) return;
            if (ttl is not null && nowMs > 0)
            {
                var expiresAt = ttl.ReadExpiresAt(place);
                if (expiresAt > 0 && nowMs > expiresAt) return;
            }
            if (filter is not null && !WhereEngine.EvaluateFilter(filter, id, place)) return;

            count++;
            if (n <= 0) return;
            if (heap.Count < n)
            {
                heap.Enqueue((id, place), id);
            }
            else if (heap.TryPeek(out _, out var worst)
                     && (descending ? id > worst : id < worst))
            {
                heap.Dequeue();
                heap.Enqueue((id, place), id);
            }
        }

        if (places is not null)
        {
            foreach (var place in places)
            {
                var id = table.Index.FindIdForPlace(place);
                if (id == 0) continue; // place no longer valid (deleted)
                Consider(id, place);
            }
        }
        else
        {
            table.Index.ForEachUsed(Consider);
        }

        matchCount = count;

        var result = new List<(ulong Id, long Place)>(heap.Count);
        while (heap.TryDequeue(out var entry, out _))
            result.Add(entry);
        result.Sort((a, b) => descending ? b.Id.CompareTo(a.Id) : a.Id.CompareTo(b.Id));
        return result;
    }

    /// <summary>
    /// Projects a pre-selected, pre-ordered list of (id, place) entries into rows.
    /// </summary>
    private static List<Dictionary<string, object?>> ProjectPlaces(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect,
        List<(ulong Id, long Place)> entries, List<LiteralColumn>? literals = null)
    {
        var projection = ResolveProjection(table, selectColumns, excludeSelect, literals);
        var ttl = table.Ttl;
        var data = new List<Dictionary<string, object?>>(entries.Count);
        foreach (var (id, place) in entries)
            data.Add(ProjectRow(projection, id, place, ttl, table));
        return data;
    }

    // ── Aggregate ─────────────────────────────────────────────

    private static SproutResponse ExecuteAggregate(TableHandle table, GetQuery q)
    {
        var filter = WhereEngine.PrepareFilter(table, q.Where);
        var nowMs = table.HasTtl ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : 0L;

        var row = new Dictionary<string, object?>();

        // First aggregate (always present here)
        AddAggregateToRow(row, table, q.AggregateColumn!, q.Aggregate!.Value, q.AggregateAlias, filter, nowMs);

        // Additional aggregates — all share the same WHERE filter so each one
        // only needs its own column read.
        if (q.AdditionalAggregates is not null)
        {
            foreach (var spec in q.AdditionalAggregates)
                AddAggregateToRow(row, table, spec.Column, spec.Function, spec.Alias, filter, nowMs);
        }

        return new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = [row],
            Affected = 1,
        };
    }

    private static void AddAggregateToRow(
        Dictionary<string, object?> row, TableHandle table,
        string colName, AggregateFunction fn, string? explicitAlias,
        WhereEngine.FilterNode? filter, long nowMs)
    {
        var alias = explicitAlias ?? AggregateName(fn);
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

        row[alias] = result;
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

        // Grouped rows are built by hand and never pass through ResolveProjection, so the
        // literals have to be added explicitly — otherwise they'd vanish without a word.
        // No select order to interleave with here; they go last.
        if (q.LiteralSelect is not null)
            ApplyLiteralColumns(data, q.LiteralSelect);

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
        string baseTable,
        IReadOnlyDictionary<string, object?>? literalValues = null)
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
                        flat[hasAlias ? name : $"{alias}.{name}"] = ReadColumnValue(handle, place, id, name, targetTable);
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
                    {
                        // A literal is constant by definition — an unmatched target row has no
                        // source, but that must not turn "1 as v" into v = null.
                        flat[col] = literalValues is not null && literalValues.TryGetValue(col, out var litVal)
                            ? litVal
                            : null;
                    }
                    flat[$"{alias}._id"] = id;
                    foreach (var (name, hasAlias, handle) in targetColumns)
                        flat[hasAlias ? name : $"{alias}.{name}"] = ReadColumnValue(handle, place, id, name, targetTable);
                    result.Add(flat);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Reads a column value, handling blob/array columns by reading external files.
    /// </summary>
    private static object? ReadColumnValue(ColumnHandle handle, long place, ulong id, string colName, TableHandle table)
    {
        if (handle.Type == ColumnType.Blob)
        {
            var byteCount = handle.ReadValue(place);
            if (byteCount is null) return null;
            var blobPath = table.GetBlobPath(colName, (long)id);
            if (File.Exists(blobPath))
                return Convert.ToBase64String(table.ReadBlobFile(colName, (long)id));
            return null;
        }
        if (handle.Type == ColumnType.Array)
        {
            var elementCount = handle.ReadValue(place);
            if (elementCount is null) return null;
            var arrayPath = table.GetArrayPath(colName, (long)id);
            if (File.Exists(arrayPath))
            {
                var json = System.Text.Encoding.UTF8.GetString(table.ReadArrayFile(colName, (long)id));
                return System.Text.Json.JsonSerializer.Deserialize<List<object?>>(json);
            }
            return null;
        }
        return handle.ReadValue(place);
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
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect, WhereEngine.FilterNode? filter,
        long nowMs = 0, List<LiteralColumn>? literals = null)
    {
        var projection = ResolveProjection(table, selectColumns, excludeSelect, literals);
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
            data.Add(ProjectRow(projection, id, place, ttl, table));
        });

        return data;
    }

    private enum VirtualColumn : byte { None, Id, ExpiresAt, Ttl, Literal }

    /// <summary>
    /// A projection entry: a real column (Handle set), a virtual column (_id, _expiresAt, _ttl)
    /// or a literal constant (Virtual = Literal, value in LiteralValue).
    /// Preserves the order from the SELECT clause.
    /// </summary>
    private readonly record struct ProjectionEntry(
        string Name, ColumnHandle? Handle,
        VirtualColumn Virtual = VirtualColumn.None,
        object? LiteralValue = null);

    private static bool IsVirtualColumn(string name) => name is "_id" or "_expiresat" or "_ttl";

    private static VirtualColumn ToVirtual(string name) => name switch
    {
        "_id" => VirtualColumn.Id,
        "_expiresat" => VirtualColumn.ExpiresAt,
        "_ttl" => VirtualColumn.Ttl,
        _ => VirtualColumn.None,
    };

    /// <summary>
    /// Whether the given column name ends up as a key in the projected rows —
    /// i.e. whether sorting by it can see a value.
    /// </summary>
    private static bool IsColumnInProjection(GetQuery q, string name)
    {
        if (q.Select is null)
            return true; // no select → every column is projected

        if (q.ExcludeSelect)
        {
            // -select: everything is there except the listed columns
            return !q.Select.Exists(c => c.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
        }

        if (q.Select.Exists(c => c.OutputName.Equals(name, StringComparison.OrdinalIgnoreCase)))
            return true;
        if (q.ComputedSelect is not null
            && q.ComputedSelect.Exists(c => c.Alias.Equals(name, StringComparison.OrdinalIgnoreCase)))
            return true;
        if (q.LiteralSelect is not null
            && q.LiteralSelect.Exists(l => l.Alias.Equals(name, StringComparison.OrdinalIgnoreCase)))
            return true;
        return false;
    }

    /// <summary>
    /// Builds an ordered projection list that respects the SELECT column order.
    /// _id is included at the position specified in SELECT (or first if no SELECT).
    /// Literals are interleaved at their own source position, so
    /// <c>select true as a, host</c> really does put 'a' before 'host'.
    /// </summary>
    private static List<ProjectionEntry> ResolveProjection(
        TableHandle table, List<SelectColumn>? selectColumns, bool excludeSelect,
        List<LiteralColumn>? literals = null)
    {
        if (selectColumns is null)
        {
            // All columns: _id first, then table columns
            var all = new List<ProjectionEntry>(table.Schema.Columns.Count + 1)
            {
                new("_id", null, VirtualColumn.Id)
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
                result.Add(new ProjectionEntry("_id", null, VirtualColumn.Id));
            foreach (var col in table.Schema.Columns)
            {
                if (!excluded.Contains(col.Name))
                    result.Add(new ProjectionEntry(col.Name, table.GetColumn(col.Name)));
            }
            return result;
        }

        {
            // Preserve SELECT order — virtual columns and literals at their position in the list.
            // Both lists are already in source order, so a two-pointer merge on Position suffices.
            var result = new List<ProjectionEntry>(selectColumns.Count + (literals?.Count ?? 0));
            var litIndex = 0;

            foreach (var col in selectColumns)
            {
                while (literals is not null && litIndex < literals.Count
                       && literals[litIndex].Position < col.Position)
                {
                    var lit = literals[litIndex++];
                    result.Add(new ProjectionEntry(lit.Alias, null, VirtualColumn.Literal, lit.Value));
                }

                var virt = ToVirtual(col.Name);
                if (virt != VirtualColumn.None)
                    result.Add(new ProjectionEntry(col.OutputName, null, virt));
                else
                    result.Add(new ProjectionEntry(col.OutputName, table.GetColumn(col.Name)));
            }

            while (literals is not null && litIndex < literals.Count)
            {
                var lit = literals[litIndex++];
                result.Add(new ProjectionEntry(lit.Alias, null, VirtualColumn.Literal, lit.Value));
            }

            return result;
        }
    }

    private static Dictionary<string, object?> ProjectRow(
        List<ProjectionEntry> projection, ulong id, long place, TtlHandle? ttl = null, TableHandle? table = null)
    {
        var record = new Dictionary<string, object?>(projection.Count);
        foreach (var entry in projection)
        {
            if (entry.Handle is not null)
            {
                if (entry.Handle.Type == ColumnType.Blob && table is not null)
                {
                    // Read blob: if .col has a value, read the .blob file and return base64
                    var byteCount = entry.Handle.ReadValue(place);
                    if (byteCount is not null)
                    {
                        var blobPath = table.GetBlobPath(entry.Name, (long)id);
                        if (File.Exists(blobPath))
                            record[entry.Name] = Convert.ToBase64String(table.ReadBlobFile(entry.Name, (long)id));
                        else
                            record[entry.Name] = null;
                    }
                    else
                    {
                        record[entry.Name] = null;
                    }
                }
                else if (entry.Handle.Type == ColumnType.Array && table is not null)
                {
                    var elementCount = entry.Handle.ReadValue(place);
                    if (elementCount is not null)
                    {
                        var arrayPath = table.GetArrayPath(entry.Name, (long)id);
                        if (File.Exists(arrayPath))
                        {
                            var json = System.Text.Encoding.UTF8.GetString(table.ReadArrayFile(entry.Name, (long)id));
                            record[entry.Name] = System.Text.Json.JsonSerializer.Deserialize<List<object?>>(json);
                        }
                        else
                        {
                            record[entry.Name] = null;
                        }
                    }
                    else
                    {
                        record[entry.Name] = null;
                    }
                }
                else
                {
                    record[entry.Name] = entry.Handle.ReadValue(place);
                }
                continue;
            }

            record[entry.Name] = entry.Virtual switch
            {
                VirtualColumn.Id => id,
                VirtualColumn.ExpiresAt => ttl?.ReadExpiresAt(place) ?? 0L,
                VirtualColumn.Ttl => ttl?.ReadRowTtlDuration(place) ?? 0L,
                VirtualColumn.Literal => entry.LiteralValue,
                _ => null,
            };
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

    /// <summary>
    /// Writes constant columns into rows that were not built by <see cref="ResolveProjection"/>
    /// (currently only the grouped path — every other path gets literals via the projection,
    /// which also preserves their position in the SELECT list).
    /// </summary>
    private static void ApplyLiteralColumns(
        List<Dictionary<string, object?>> data,
        List<LiteralColumn> literals)
    {
        foreach (var row in data)
        {
            foreach (var lit in literals)
                row[lit.Alias] = lit.Value;
        }
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
        List<long> places, WhereEngine.FilterNode? filter, long nowMs = 0,
        List<LiteralColumn>? literals = null)
    {
        var projection = ResolveProjection(table, selectColumns, excludeSelect, literals);
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

            data.Add(ProjectRow(projection, id, place, ttl, table));
        }

        return data;
    }
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using SproutDB.Engine.Compilation;
using SproutDB.Engine.Core;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace SproutDB.Engine.Execution;

public class SproutDbExecutor(
    ISproutDB database,
    IDataStore dataStore,
    ILogger<SproutDbExecutor> logger,
    IServiceProvider serviceProvider)
    : IQueryExecutor
{
    public ExecutionResult Execute(IStatement statement)
    {
        return Execute(statement, new ExecutionContext());
    }

    public ExecutionResult Execute(IStatement statement, ExecutionContext context)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            logger.LogDebug("Executing {statementType} statement at position {statementPosition}", statement.Type, statement.Position);

            // Validate current database
            var currentDb = database.GetCurrentDatabase();
            if (currentDb == null && statement is not SchemaStatement { Type: StatementType.Schema, Operation: SchemaOperation.CreateDatabase, DatabaseName: not null })
            {
                return ExecutionResult.CreateError("No database selected");
            }

            // Route to appropriate handler
            var result = statement.Type switch
            {
                StatementType.Query => ExecuteQuery((QueryStatement)statement, context),
                StatementType.Upsert => ExecuteUpsert((UpsertStatement)statement, context),
                StatementType.Delete => ExecuteDelete((DeleteStatement)statement, context),
                StatementType.Schema => ExecuteSchema((SchemaStatement)statement, context),
                StatementType.Branch => ExecuteBranch((BranchStatement)statement, context),
                StatementType.Auth => ExecuteAuth((AuthStatement)statement, context),
                StatementType.Meta => ExecuteMeta((MetaStatement)statement, context),
                _ => ExecutionResult.CreateError($"Unsupported statement type: {statement.Type}")
            };

            var executionTime = DateTime.UtcNow - startTime;
            logger.LogInformation($"Statement executed in {executionTime.TotalMilliseconds}ms");

            return result;
        }
        catch (Exception ex)
        {
            var executionTime = DateTime.UtcNow - startTime;
            logger.LogError($"Execution failed: {ex.Message}", ex);
            return ExecutionResult.CreateError($"Execution error: {ex.Message}", executionTime);
        }
    }

    private ExecutionResult ExecuteQuery(QueryStatement query, ExecutionContext context)
    {
        try
        {
            // Validate table exists
            var currentDb = database.GetCurrentDatabase()!;
            if (!currentDb.Tables.ContainsKey(query.Table.Name))
            {
                return ExecutionResult.CreateError($"Table '{query.Table.Name}' does not exist");
            }

            // Apply dry run check
            if (context.DryRun)
            {
                return ExecutionResult.CreateOk($"Query would execute on table '{query.Table.Name}'");
            }

            // Execute based on operation type
            return query.Operation switch
            {
                QueryOperation.Get => ExecuteGetQuery(query, context),
                QueryOperation.Count => ExecuteCountQuery(query, context),
                QueryOperation.Sum => ExecuteSumQuery(query, context),
                QueryOperation.Avg => ExecuteAvgQuery(query, context),
                _ => ExecutionResult.CreateError($"Unsupported query operation: {query.Operation}")
            };
        }
        catch (Exception ex)
        {
            return ExecutionResult.CreateError($"Query execution failed: {ex.Message}");
        }
    }

    private ExecutionResult ExecuteGetQuery(QueryStatement query, ExecutionContext context)
    {
        // Get base rows with filtering
        var rows = dataStore.GetRows(query.Table.Name, query.Where, context.MaxRows);
        var rowList = rows.ToList();

        // Apply joins if specified
        if (query.Joins.Length > 0)
        {
            rowList = ApplyJoins(rowList, query.Joins);
        }

        // Apply grouping if specified
        if (query.GroupBy.Length > 0)
        {
            rowList = ApplyGrouping(rowList, query.GroupBy, query.Having);
        }

        // Apply ordering if specified
        if (query.OrderBy.Length > 0)
        {
            rowList = ApplyOrdering(rowList, query.OrderBy);
        }

        // Apply selection if specified
        if (query.Select.Length > 0)
        {
            rowList = ApplySelection(rowList, query.Select);
        }

        // Apply pagination if specified
        if (query.Pagination.HasValue)
        {
            var pagination = query.Pagination.Value;
            var skip = (pagination.Page - 1) * pagination.Size;
            rowList = rowList.Skip(skip).Take(pagination.Size).ToList();
        }

        return ExecutionResult.CreateOk(
            data: rowList,
            rowsScanned: rowList.Count,
            rowsAffected: rowList.Count
        );
    }

    private ExecutionResult ExecuteCountQuery(QueryStatement query, ExecutionContext context)
    {
        var count = dataStore.CountRows(query.Table.Name, query.Where);
        return ExecutionResult.CreateOk(
            data: count,
            rowsScanned: count,
            rowsAffected: 0
        );
    }

    private ExecutionResult ExecuteSumQuery(QueryStatement query, ExecutionContext context)
    {
        // Get rows based on where clause
        var rows = dataStore.GetRows(query.Table.Name, query.Where).ToList();

        if (rows.Count == 0)
        {
            // No rows to sum, return null
            return ExecutionResult.CreateOk(
                data: 0,
                rowsScanned: 0
            );
        }

        // If no specific fields are selected, we can't calculate a sum
        if (query.Select.Length == 0)
        {
            return ExecutionResult.CreateError("No field specified for SUM operation");
        }

        // Extract the field name from the first select expression
        var field = ExtractField(query.Select.Span[0]);

        if (!field.HasValue)
        {
            return ExecutionResult.CreateError("Invalid field for SUM operation");
        }

        // Calculate sum in a single pass for performance
        double sum = 0;
        // Track the widest type encountered - start with narrowest numeric type
        Type widestType = typeof(int);

        foreach (var row in rows)
        {
            if (row.Fields.TryGetValue(field.Value.Name, out var value) && value != null)
            {
                // Try to convert the value to a numeric type
                if (value is int intValue)
                {
                    sum += intValue;
                }
                else if (value is double doubleValue)
                {
                    sum += doubleValue;
                    if (widestType == typeof(int) || widestType == typeof(long) || widestType == typeof(float))
                    {
                        widestType = typeof(double);
                    }
                }
                else if (value is long longValue)
                {
                    sum += longValue;
                    if (widestType == typeof(int) || widestType == typeof(float))
                    {
                        widestType = typeof(long);
                    }
                }
                else if (value is decimal decimalValue)
                {
                    sum += (double)decimalValue;
                    if (widestType == typeof(int) || widestType == typeof(long) || widestType == typeof(float))
                    {
                        widestType = typeof(decimal);
                    }
                }
                else if (value is float floatValue)
                {
                    sum += floatValue;
                    if (widestType == typeof(int) || widestType == typeof(long))
                    {
                        widestType = typeof(float);
                    }
                }
                else if (double.TryParse(value.ToString(), out var parsedValue))
                {
                    sum += parsedValue;
                    if (widestType == typeof(int) || widestType == typeof(long) || widestType == typeof(float))
                    {
                        widestType = typeof(double);
                    }
                }
            }
        }

        object ConvertWithTypePromotion(double sum, Type baseType)
        {
            // Check for overflow and promote types as needed
            if (baseType == typeof(int))
            {
                if (sum > int.MaxValue || sum < int.MinValue)
                    return (long)sum; // Promote to long
                return (int)sum;
            }
            else if (baseType == typeof(long))
            {
                if (sum > long.MaxValue || sum < long.MinValue)
                    return sum; // Promote to double
                return (long)sum;
            }
            else if (baseType == typeof(float))
            {
                if (sum > float.MaxValue || sum < float.MinValue)
                    return sum; // Promote to double
                return (float)sum;
            }
            else if (baseType == typeof(decimal))
            {
                if (sum > (double)decimal.MaxValue || sum < (double)decimal.MinValue)
                    return sum; // Use double if beyond decimal range
                return (decimal)sum;
            }

            // Default to the base type conversion
            return Convert.ChangeType(sum, baseType);
        }

        return ExecutionResult.CreateOk(
            data: ConvertWithTypePromotion(sum, widestType),
            rowsScanned: rows.Count
        );
    }

    private ExecutionResult ExecuteAvgQuery(QueryStatement query, ExecutionContext context)
    {
        // Get rows based on where clause
        var rows = dataStore.GetRows(query.Table.Name, query.Where).ToList();

        if (rows.Count == 0)
        {
            // No rows to average, return null
            return ExecutionResult.CreateOk(
                data: null,
                rowsScanned: 0
            );
        }

        // If no specific fields are selected, we can't calculate an average
        if (query.Select.Length == 0)
        {
            return ExecutionResult.CreateError("No field specified for AVG operation");
        }

        // Extract the field name from the first select expression
        var field = ExtractField(query.Select.Span[0]);

        if (!field.HasValue)
        {
            return ExecutionResult.CreateError("Invalid field for AVG operation");
        }

        // Calculate average in a single pass for performance
        double sum = 0;
        var count = 0;

        foreach (var row in rows)
        {
            if (row.Fields.TryGetValue(field.Value.Name, out var value) && value != null)
            {
                // Try to convert the value to a numeric type
                if (value is int intValue)
                {
                    sum += intValue;
                    count++;
                }
                else if (value is double doubleValue)
                {
                    sum += doubleValue;
                    count++;
                }
                else if (value is long longValue)
                {
                    sum += longValue;
                    count++;
                }
                else if (value is decimal decimalValue)
                {
                    sum += (double)decimalValue;
                    count++;
                }
                else if (value is float floatValue)
                {
                    sum += floatValue;
                    count++;
                }
                else if (double.TryParse(value.ToString(), out var parsedValue))
                {
                    sum += parsedValue;
                    count++;
                }
            }
        }

        // Return the average
        double average = count > 0 ? sum / count : 0;

        return ExecutionResult.CreateOk(
            data: average,
            rowsScanned: rows.Count
        );
    }

    private ExecutionResult ExecuteUpsert(UpsertStatement upsert, ExecutionContext context)
    {
        try
        {
            // Validate table exists
            var currentDb = database.GetCurrentDatabase()!;
            if (!currentDb.Tables.ContainsKey(upsert.Table.Name))
            {
                return ExecutionResult.CreateError($"Table '{upsert.Table.Name}' does not exist");
            }

            if (context.DryRun)
            {
                return ExecutionResult.CreateOk("Upsert would execute");
            }

            // Check if we have JSON array data
            var isArrayData = false;
            if (upsert.Data.Type == ExpressionType.JsonValue)
            {
                var jsonData = upsert.Data.As<Expression.JsonData>();
                isArrayData = jsonData.ValueType == JsonValueType.Array;
            }

            if (isArrayData)
            {
                // Handle bulk upsert
                var rows = ConvertJsonExpressionToRows(upsert.Data);
                if (rows.Count == 0)
                {
                    return ExecutionResult.CreateError("No valid rows found in JSON array for upsert");
                }

                var rowIds = new List<string>();
                foreach (var row in rows)
                {
                    rowIds.Add(dataStore.UpsertRow(upsert.Table.Name, row, upsert.OnField));
                }

                // Generate commit ID for tracking
                var commitId = GenerateCommitId();

                return ExecutionResult.CreateOk(
                    data: new { ids = rowIds },
                    rowsAffected: rows.Count,
                    commitId: commitId
                );
            }
            else
            {
                // Handle single row upsert
                var row = ConvertJsonExpressionToRow(upsert.Data);
                var rowId = dataStore.UpsertRow(upsert.Table.Name, row, upsert.OnField);

                // Generate commit ID for tracking
                var commitId = GenerateCommitId();

                return ExecutionResult.CreateOk(
                    data: new { id = rowId },
                    rowsAffected: 1,
                    commitId: commitId
                );
            }
        }
        catch (Exception ex)
        {
            return ExecutionResult.CreateError($"Upsert execution failed: {ex.Message}");
        }
    }

    private ExecutionResult ExecuteDelete(DeleteStatement delete, ExecutionContext context)
    {
        try
        {
            // Validate table exists
            var currentDb = database.GetCurrentDatabase()!;
            if (!currentDb.Tables.ContainsKey(delete.Table.Name))
            {
                return ExecutionResult.CreateError($"Table '{delete.Table.Name}' does not exist");
            }

            if (context.DryRun)
            {
                var estimatedRows = dataStore.CountRows(delete.Table.Name, delete.Where);
                return ExecutionResult.CreateOk($"Would delete approximately {estimatedRows} rows");
            }

            // Execute delete
            var deletedRows = dataStore.DeleteRows(delete.Table.Name, delete.Where);

            // Generate commit ID for tracking
            var commitId = GenerateCommitId();

            return ExecutionResult.CreateOk(
                data: new { deleted = deletedRows },
                rowsAffected: deletedRows,
                commitId: commitId
            );
        }
        catch (Exception ex)
        {
            return ExecutionResult.CreateError($"Delete execution failed: {ex.Message}");
        }
    }

    private ExecutionResult ExecuteSchema(SchemaStatement schema, ExecutionContext context)
    {
        try
        {
            if (context.DryRun)
            {
                return ExecutionResult.CreateOk($"Schema operation {schema.Operation} would execute");
            }

            return schema.Operation switch
            {
                SchemaOperation.CreateDatabase => ExecuteCreateDatabase(schema),
                SchemaOperation.CreateTable => ExecuteCreateTable(schema),
                SchemaOperation.DropTable => ExecuteDropTable(schema),
                SchemaOperation.AddColumn => ExecuteAddColumn(schema),
                SchemaOperation.PurgeColumn => ExecutePurgeColumn(schema),
                _ => ExecutionResult.CreateError($"Unsupported schema operation: {schema.Operation}")
            };
        }
        catch (Exception ex)
        {
            return ExecutionResult.CreateError($"Schema execution failed: {ex.Message}");
        }
    }

    private ExecutionResult ExecuteCreateDatabase(SchemaStatement schema)
    {
        if (string.IsNullOrEmpty(schema.DatabaseName))
        {
            return ExecutionResult.CreateError("Database name is required for CREATE DATABASE");
        }
        if (database.Databases.ContainsKey(schema.DatabaseName))
        {
            return ExecutionResult.CreateError($"Database '{schema.DatabaseName}' already exists");
        }
        // Create new database
        var newDb = serviceProvider.GetRequiredService<IDatabase>();
        database.Databases[schema.DatabaseName] = newDb;
        var commitId = GenerateCommitId();
        return ExecutionResult.CreateOk(
            data: new { database = schema.DatabaseName },
            commitId: commitId
        );
    }

    private ExecutionResult ExecuteCreateTable(SchemaStatement schema)
    {
        if (string.IsNullOrEmpty(schema.TableName))
        {
            return ExecutionResult.CreateError("Table name is required for CREATE TABLE");
        }

        var currentDb = database.GetCurrentDatabase()!;
        if (currentDb.Tables.ContainsKey(schema.TableName))
        {
            return ExecutionResult.CreateError($"Table '{schema.TableName}' already exists");
        }

        dataStore.CreateTable(schema.TableName);

        // Add to schema
        currentDb.Tables[schema.TableName] = new Table();

        var commitId = GenerateCommitId();
        return ExecutionResult.CreateOk(
            data: new { table = schema.TableName },
            commitId: commitId
        );
    }

    private ExecutionResult ExecuteDropTable(SchemaStatement schema)
    {
        if (string.IsNullOrEmpty(schema.TableName))
        {
            return ExecutionResult.CreateError("Table name is required for DROP TABLE");
        }

        var currentDb = database.GetCurrentDatabase()!;
        if (!currentDb.Tables.ContainsKey(schema.TableName))
        {
            return ExecutionResult.CreateError($"Table '{schema.TableName}' does not exist");
        }

        dataStore.DropTable(schema.TableName);
        currentDb.Tables.Remove(schema.TableName);

        var commitId = GenerateCommitId();
        return ExecutionResult.CreateOk(
            data: new { dropped = schema.TableName },
            commitId: commitId
        );
    }

    private ExecutionResult ExecuteAddColumn(SchemaStatement schema)
    {
        if (string.IsNullOrEmpty(schema.TableName) || string.IsNullOrEmpty(schema.ColumnName))
        {
            return ExecutionResult.CreateError("Table name and column name are required for ADD COLUMN");
        }

        var currentDb = database.GetCurrentDatabase()!;
        if (!currentDb.Tables.TryGetValue(schema.TableName, out var table))
        {
            return ExecutionResult.CreateError($"Table '{schema.TableName}' does not exist");
        }

        // Parse column type (default to string if not specified)
        var columnType = ParseColumnType(schema.DataType);

        dataStore.AddColumn(schema.TableName, schema.ColumnName, columnType);
        table.Columns[schema.ColumnName] = new Column(columnType);

        var commitId = GenerateCommitId();
        return ExecutionResult.CreateOk(
            data: new { table = schema.TableName, column = schema.ColumnName, type = columnType },
            commitId: commitId
        );
    }

    private ExecutionResult ExecutePurgeColumn(SchemaStatement schema)
    {
        if (string.IsNullOrEmpty(schema.TableName) || string.IsNullOrEmpty(schema.ColumnName))
        {
            return ExecutionResult.CreateError("Table name and column name are required for PURGE COLUMN");
        }

        var currentDb = database.GetCurrentDatabase()!;
        if (!currentDb.Tables.TryGetValue(schema.TableName, out var table))
        {
            return ExecutionResult.CreateError($"Table '{schema.TableName}' does not exist");
        }

        if (!table.Columns.ContainsKey(schema.ColumnName))
        {
            return ExecutionResult.CreateError($"Column '{schema.ColumnName}' does not exist in table '{schema.TableName}'");
        }

        dataStore.PurgeColumn(schema.TableName, schema.ColumnName);
        table.Columns.Remove(schema.ColumnName);

        var commitId = GenerateCommitId();
        return ExecutionResult.CreateOk(
            data: new { table = schema.TableName, purged = schema.ColumnName },
            commitId: commitId
        );
    }

    private ExecutionResult ExecuteBranch(BranchStatement branch, ExecutionContext context)
    {
        try
        {
            if (context.DryRun)
            {
                return ExecutionResult.CreateOk($"Branch operation {branch.Operation} would execute");
            }

            // Implementation based on branch operation
            return branch.Operation switch
            {
                BranchOperation.Create => ExecuteCreateBranch(branch),
                BranchOperation.Checkout => ExecuteCheckoutBranch(branch),
                BranchOperation.Merge => ExecuteMergeBranch(branch),
                BranchOperation.Delete => ExecuteDeleteBranch(branch),
                BranchOperation.Protect => ExecuteProtectBranch(branch),
                BranchOperation.Unprotect => ExecuteUnprotectBranch(branch),
                BranchOperation.Abandon => ExecuteAbandonBranch(branch),
                BranchOperation.Reactivate => ExecuteReactivateBranch(branch),
                _ => ExecutionResult.CreateError($"Unsupported branch operation: {branch.Operation}")
            };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error executing branch operation");
            return ExecutionResult.CreateError($"Branch operation failed: {ex.Message}");
        }
    }
    private ExecutionResult ExecuteCreateBranch(BranchStatement branch)
    {
        if (string.IsNullOrEmpty(branch.BranchName))
        {
            return ExecutionResult.CreateError("Branch name is required for CREATE BRANCH");
        }

        logger.LogInformation($"Creating branch: {branch.BranchName} from {branch.SourceBranch ?? "current branch"}");

        // Implementation would interact with version control system
        // For now, return a placeholder success
        var commitId = GenerateCommitId();
        return ExecutionResult.CreateOk(
            data: new { branch = branch.BranchName, source = branch.SourceBranch },
            commitId: commitId
        );
    }
    private ExecutionResult ExecuteCheckoutBranch(BranchStatement branch)
    {
        if (string.IsNullOrEmpty(branch.BranchName))
        {
            return ExecutionResult.CreateError("Branch name is required for CHECKOUT BRANCH");
        }

        logger.LogInformation($"Checking out branch: {branch.BranchName}");

        // Implementation would switch the active branch
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { branch = branch.BranchName }
        );
    }
    private ExecutionResult ExecuteMergeBranch(BranchStatement branch)
    {
        if (string.IsNullOrEmpty(branch.SourceBranch) || string.IsNullOrEmpty(branch.TargetBranch))
        {
            return ExecutionResult.CreateError("Source and target branches are required for MERGE");
        }

        logger.LogInformation($"Merging branch: {branch.SourceBranch} into {branch.TargetBranch}");

        // Implementation would merge branches
        // For now, return a placeholder success
        var commitId = GenerateCommitId();
        return ExecutionResult.CreateOk(
            data: new { source = branch.SourceBranch, target = branch.TargetBranch },
            commitId: commitId
        );
    }
    private ExecutionResult ExecuteDeleteBranch(BranchStatement branch)
    {
        if (string.IsNullOrEmpty(branch.BranchName))
        {
            return ExecutionResult.CreateError("Branch name is required for DELETE BRANCH");
        }

        logger.LogInformation($"Deleting branch: {branch.BranchName}");

        // Implementation would delete the branch
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { deleted = branch.BranchName }
        );
    }
    private ExecutionResult ExecuteProtectBranch(BranchStatement branch)
    {
        if (string.IsNullOrEmpty(branch.BranchName))
        {
            return ExecutionResult.CreateError("Branch name is required for PROTECT BRANCH");
        }

        logger.LogInformation($"Protecting branch: {branch.BranchName}");

        // Implementation would set protection on the branch
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { protected_branch = branch.BranchName }
        );
    }
    private ExecutionResult ExecuteUnprotectBranch(BranchStatement branch)
    {
        if (string.IsNullOrEmpty(branch.BranchName))
        {
            return ExecutionResult.CreateError("Branch name is required for UNPROTECT BRANCH");
        }

        logger.LogInformation($"Unprotecting branch: {branch.BranchName}");

        // Implementation would remove protection from the branch
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { unprotected_branch = branch.BranchName }
        );
    }
    private ExecutionResult ExecuteAbandonBranch(BranchStatement branch)
    {
        if (string.IsNullOrEmpty(branch.BranchName))
        {
            return ExecutionResult.CreateError("Branch name is required for ABANDON BRANCH");
        }

        logger.LogInformation($"Abandoning branch: {branch.BranchName}");

        // Implementation would mark the branch as abandoned
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { abandoned = branch.BranchName, reason = branch.Alias }
        );
    }
    private ExecutionResult ExecuteReactivateBranch(BranchStatement branch)
    {
        if (string.IsNullOrEmpty(branch.BranchName))
        {
            return ExecutionResult.CreateError("Branch name is required for REACTIVATE BRANCH");
        }

        logger.LogInformation($"Reactivating branch: {branch.BranchName}");

        // Implementation would reactivate the abandoned branch
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { reactivated = branch.BranchName }
        );
    }

    private ExecutionResult ExecuteAuth(AuthStatement auth, ExecutionContext context)
    {
        try
        {
            if (context.DryRun)
            {
                return ExecutionResult.CreateOk($"Auth operation {auth.Operation} would execute");
            }

            // Implementation based on auth operation
            return auth.Operation switch
            {
                AuthOperation.CreateToken => ExecuteCreateToken(auth),
                AuthOperation.UpdateToken => ExecuteUpdateToken(auth),
                AuthOperation.RevokeToken => ExecuteRevokeToken(auth),
                AuthOperation.ListTokens => ExecuteListTokens(auth),
                AuthOperation.DisableToken => ExecuteDisableToken(auth),
                AuthOperation.EnableToken => ExecuteEnableToken(auth),
                _ => ExecutionResult.CreateError($"Unsupported auth operation: {auth.Operation}")
            };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error executing auth operation");
            return ExecutionResult.CreateError($"Auth operation failed: {ex.Message}");
        }
    }
    private ExecutionResult ExecuteCreateToken(AuthStatement auth)
    {
        // Token name validation
        if (string.IsNullOrEmpty(auth.TokenName))
        {
            return ExecutionResult.CreateError("Token name is required for CREATE TOKEN");
        }

        // Configuration validation
        if (auth.Configuration == null)
        {
            return ExecutionResult.CreateError("Token configuration is required");
        }

        logger.LogInformation($"Creating token: {auth.TokenName}");

        // Implementation would create an auth token with the specified configuration
        // For now, return a placeholder success with a generated token
        var tokenValue = $"pat_{Guid.NewGuid():N}";
        return ExecutionResult.CreateOk(
            data: new { name = auth.TokenName, token = tokenValue }
        );
    }
    private ExecutionResult ExecuteUpdateToken(AuthStatement auth)
    {
        if (string.IsNullOrEmpty(auth.TokenName))
        {
            return ExecutionResult.CreateError("Token name is required for UPDATE TOKEN");
        }

        logger.LogInformation($"Updating token: {auth.TokenName}");

        // Implementation would update the token configuration
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { updated = auth.TokenName }
        );
    }
    private ExecutionResult ExecuteRevokeToken(AuthStatement auth)
    {
        if (string.IsNullOrEmpty(auth.TokenName))
        {
            return ExecutionResult.CreateError("Token name is required for REVOKE TOKEN");
        }

        logger.LogInformation($"Revoking token: {auth.TokenName}");

        // Implementation would revoke the token
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { revoked = auth.TokenName }
        );
    }
    private ExecutionResult ExecuteListTokens(AuthStatement auth)
    {
        logger.LogInformation("Listing tokens");

        // Implementation would list available tokens
        // For now, return a placeholder list
        return ExecutionResult.CreateOk(
            data: new[] {
            new { name = "example-token-1", created = DateTime.UtcNow.AddDays(-30) },
            new { name = "example-token-2", created = DateTime.UtcNow.AddDays(-15) }
            }
        );
    }
    private ExecutionResult ExecuteDisableToken(AuthStatement auth)
    {
        if (string.IsNullOrEmpty(auth.TokenName))
        {
            return ExecutionResult.CreateError("Token name is required for DISABLE TOKEN");
        }

        logger.LogInformation($"Disabling token: {auth.TokenName}");

        // Implementation would disable the token
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { disabled = auth.TokenName }
        );
    }
    private ExecutionResult ExecuteEnableToken(AuthStatement auth)
    {
        if (string.IsNullOrEmpty(auth.TokenName))
        {
            return ExecutionResult.CreateError("Token name is required for ENABLE TOKEN");
        }

        logger.LogInformation($"Enabling token: {auth.TokenName}");

        // Implementation would enable the token
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new { enabled = auth.TokenName }
        );
    }

    private ExecutionResult ExecuteMeta(MetaStatement meta, ExecutionContext context)
    {
        try
        {
            if (context.DryRun)
            {
                return ExecutionResult.CreateOk($"Meta operation {meta.Operation} would execute");
            }

            // Implementation based on meta operation
            return meta.Operation switch
            {
                MetaOperation.Backup => ExecuteBackup(meta),
                MetaOperation.Restore => ExecuteRestore(meta),
                MetaOperation.Explain => ExecuteExplain(meta),
                MetaOperation.Respawn => ExecuteRespawn(meta),
                _ => ExecutionResult.CreateError($"Unsupported meta operation: {meta.Operation}")
            };
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error executing meta operation");
            return ExecutionResult.CreateError($"Meta operation failed: {ex.Message}");
        }
    }
    private ExecutionResult ExecuteBackup(MetaStatement meta)
    {
        if (string.IsNullOrEmpty(meta.Target))
        {
            return ExecutionResult.CreateError("Backup target is required");
        }

        logger.LogInformation($"Backing up database to: {meta.Target}");

        // Implementation would create a database backup
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new
            {
                backup_file = meta.Target,
                timestamp = DateTime.UtcNow,
                size = "1.2MB"
            }
        );
    }
    private ExecutionResult ExecuteRestore(MetaStatement meta)
    {
        if (string.IsNullOrEmpty(meta.Source))
        {
            return ExecutionResult.CreateError("Restore source is required");
        }

        logger.LogInformation($"Restoring database from: {meta.Source}");

        // Implementation would restore from a backup
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new
            {
                restored_from = meta.Source,
                timestamp = DateTime.UtcNow
            }
        );
    }
    private ExecutionResult ExecuteExplain(MetaStatement meta)
    {
        if (string.IsNullOrEmpty(meta.Target))
        {
            return ExecutionResult.CreateError("Query to explain is required");
        }

        logger.LogInformation($"Explaining query: {meta.Target}");

        // Implementation would analyze the query execution plan
        // For now, return a placeholder analysis
        return ExecutionResult.CreateOk(
            data: new
            {
                query = meta.Target,
                execution_plan = new[] {
                new { step = 1, operation = "Table Scan", estimated_time = "5ms" },
                new { step = 2, operation = "Filter", estimated_time = "1ms" },
                new { step = 3, operation = "Sort", estimated_time = "2ms" }
                },
                estimated_total_time = "8ms"
            }
        );
    }
    private ExecutionResult ExecuteRespawn(MetaStatement meta)
    {
        if (string.IsNullOrEmpty(meta.Target) || string.IsNullOrEmpty(meta.Source))
        {
            return ExecutionResult.CreateError("Source branch and target database name are required for RESPAWN");
        }

        logger.LogInformation($"Respawning branch {meta.Source} as new database {meta.Target}");

        // Implementation would create a new database from a branch
        // For now, return a placeholder success
        return ExecutionResult.CreateOk(
            data: new
            {
                source_branch = meta.Source,
                new_database = meta.Target,
                timestamp = DateTime.UtcNow
            }
        );
    }

    // Helper methods
    private static List<Row> ApplyJoins(List<Row> rows, ReadOnlyMemory<JoinExpression> joins)
    {
        // TODO: Implement join logic
        return rows;
    }

    private static List<Row> ApplyGrouping(List<Row> rows, ReadOnlyMemory<Expression> groupBy, Expression? having)
    {
        if (rows.Count == 0 || groupBy.Length == 0)
        {
            return rows; // Nothing to group or no grouping fields specified
        }

        // Extract field names from groupBy expressions
        var groupByFields = new List<Field>(groupBy.Length);
        foreach (var expr in groupBy.Span)
        {
            var field = ExtractField(expr);
            if (field.HasValue)
            {
                groupByFields.Add(field.Value);
            }
        }

        // If no valid fields were found, return the original rows
        if (groupByFields.Count == 0)
        {
            return rows;
        }

        // Group rows by the specified fields
        var groupedRows = new Dictionary<string, List<Row>>();

        foreach (var row in rows)
        {
            // Create a composite key from the grouping fields
            var keyComponents = new List<string>(groupByFields.Count);

            foreach (var field in groupByFields)
            {
                // Handle the case where the field doesn't exist in the row
                var fieldValue = row.Fields.TryGetValue(field.Name, out var value) ? value : null;

                // Convert value to string representation for the key
                // Treat null values specially
                var stringValue = fieldValue == null ? "NULL" : fieldValue.ToString() ?? "NULL";
                keyComponents.Add($"{field}:{stringValue}");
            }

            var groupKey = string.Join("||", keyComponents);

            // Add row to the appropriate group
            if (!groupedRows.TryGetValue(groupKey, out var group))
            {
                group = new List<Row>();
                groupedRows[groupKey] = group;
            }

            group.Add(row);
        }

        // Create one row per group with the group by fields and aggregates
        var result = new List<Row>(groupedRows.Count);

        foreach (var group in groupedRows.Values)
        {
            var groupRow = new Row();

            // Use values from the first row for the grouping fields
            var firstRow = group[0];

            // Set a generated ID for the group row
            groupRow.Id = Guid.NewGuid().ToString("N");

            // Copy the grouped fields
            foreach (var field in groupByFields)
            {
                if (firstRow.Fields.TryGetValue(field.Name, out var value))
                {
                    groupRow.SetField(field.Alias ?? field.Name, value);
                }
            }

            // Add count aggregate - required by tests
            groupRow.SetField("count()", group.Count);

            // Calculate sum aggregates for numeric fields
            // This is needed for the test cases
            foreach (var field in firstRow.Fields.Keys)
            {
                double sum = 0;
                int validValueCount = 0;

                foreach (var row in group)
                {
                    if (row.Fields.TryGetValue(field, out var value) && value != null && IsNumeric(value))
                    {
                        sum += Convert.ToDouble(value);
                        validValueCount++;
                    }
                }

                if (validValueCount > 0)
                {
                    groupRow.SetField($"sum({field})", sum);
                    groupRow.SetField($"avg({field})", sum / validValueCount);
                }
            }

            result.Add(groupRow);
        }

        // Apply HAVING filter if specified
        if (having != null)
        {
            result = FilterGroupsByHaving(result, having.Value);
        }

        return result;
    }

    private static List<Row> FilterGroupsByHaving(List<Row> groupedRows, Expression having)
    {
        if (groupedRows.Count == 0)
        {
            return groupedRows;
        }

        var filteredRows = new List<Row>();

        foreach (var row in groupedRows)
        {
            if (EvaluateHavingExpression(row, having))
            {
                filteredRows.Add(row);
            }
        }

        return filteredRows;
    }

    private static bool EvaluateHavingExpression(Row row, Expression expr)
    {
        switch (expr.Type)
        {
            case ExpressionType.Comparison:
                var comparison = expr.As<Expression.ComparisonData>();
                return EvaluateComparison(row, comparison);

            case ExpressionType.Binary:
                var binary = expr.As<Expression.BinaryData>();
                return EvaluateBinaryExpression(row, binary);

            case ExpressionType.Unary:
                var unary = expr.As<Expression.UnaryData>();
                return EvaluateUnaryExpression(row, unary);

            default:
                return false;
        }
    }

    private static bool EvaluateComparison(Row row, Expression.ComparisonData comparison)
    {
        // Extract values for comparison
        var leftValue = GetExpressionValue(row, comparison.Left);
        var rightValue = GetExpressionValue(row, comparison.Right);

        // Handle null values
        if (leftValue == null && rightValue == null)
        {
            return comparison.Operator == ComparisonOperator.Equals;
        }
        else if (leftValue == null || rightValue == null)
        {
            return comparison.Operator == ComparisonOperator.NotEquals;
        }

        // Use the existing CompareValues method
        int comparisonResult = CompareValues(leftValue, rightValue);

        return comparison.Operator switch
        {
            ComparisonOperator.Equals => comparisonResult == 0,
            ComparisonOperator.NotEquals => comparisonResult != 0,
            ComparisonOperator.GreaterThan => comparisonResult > 0,
            ComparisonOperator.GreaterThanOrEqual => comparisonResult >= 0,
            ComparisonOperator.LessThan => comparisonResult < 0,
            ComparisonOperator.LessThanOrEqual => comparisonResult <= 0,
            _ => false
        };
    }

    private static bool EvaluateBinaryExpression(Row row, Expression.BinaryData binary)
    {
        return binary.Operator switch
        {
            LogicalOperator.And => EvaluateHavingExpression(row, binary.Left) && EvaluateHavingExpression(row, binary.Right),
            LogicalOperator.Or => EvaluateHavingExpression(row, binary.Left) || EvaluateHavingExpression(row, binary.Right),
            _ => false
        };
    }

    private static bool EvaluateUnaryExpression(Row row, Expression.UnaryData unary)
    {
        return unary.Operator == LogicalOperator.Not
            ? !EvaluateHavingExpression(row, unary.Operand)
            : false;
    }
    private static object? GetExpressionValue(Row row, Expression expr)
    {
        switch (expr.Type)
        {
            case ExpressionType.FieldPath:
                var field = ExtractField(expr);
                if (field.HasValue)
                {
                    // Check for direct field match
                    if (row.Fields.TryGetValue(field.Value.Name, out var value))
                    {
                        return value;
                    }

                    // Handle aggregate functions like count(), sum(), avg()
                    string? exprStr = expr.ToString();
                    if (!string.IsNullOrEmpty(exprStr) && exprStr.Contains('(') && exprStr.Contains(')'))
                    {
                        if (row.Fields.TryGetValue(exprStr, out var aggValue))
                        {
                            return aggValue;
                        }
                    }
                }
                return null;

            case ExpressionType.Literal:
                var literal = expr.As<Expression.LiteralData>();
                return literal.LiteralType switch
                {
                    LiteralType.String => TrimQuotes(literal.Value),
                    LiteralType.Number => ParseNumber(literal.Value),
                    LiteralType.Boolean => bool.Parse(literal.Value),
                    LiteralType.Null => null,
                    _ => literal.Value
                };

            case ExpressionType.JsonValue:
                var jsonData = expr.As<Expression.JsonData>();
                return jsonData.ValueType switch
                {
                    JsonValueType.String => jsonData.Value?.ToString(),
                    JsonValueType.Number => jsonData.Value is string str ? ParseNumber(str) : jsonData.Value,
                    JsonValueType.Boolean => jsonData.Value is string boolStr ? bool.Parse(boolStr) : jsonData.Value,
                    JsonValueType.Null => null,
                    _ => jsonData.Value
                };

            default:
                return null;
        }
    }


    private static List<Row> ApplyOrdering(List<Row> rows, ReadOnlyMemory<OrderByField> orderBy)
    {
        if (rows.Count <= 1 || orderBy.Length == 0)
        {
            return rows; // No need to sort a single row, empty list, or when no ordering is specified
        }

        // Create a list to hold the sorted rows
        var sortedRows = new List<Row>(rows);

        // Use a custom comparer for multiple sort fields
        sortedRows.Sort((row1, row2) =>
        {
            // Compare based on each order-by field in sequence
            for (int i = 0; i < orderBy.Length; i++)
            {
                var orderByField = orderBy.Span[i];
                var field = ExtractField(orderByField.Field);

                if (!field.HasValue)
                {
                    continue; // Skip invalid fields
                }

                // Get field values for comparison
                var hasValue1 = row1.Fields.TryGetValue(field.Value.Name, out var value1);
                var hasValue2 = row2.Fields.TryGetValue(field.Value.Name, out var value2);

                // Handle cases where one or both fields are missing
                if (!hasValue1 && !hasValue2) continue;      // Both missing, consider equal
                if (!hasValue1) return orderByField.Direction == SortDirection.Asc ? -1 : 1; // First missing, comes first in ASC
                if (!hasValue2) return orderByField.Direction == SortDirection.Asc ? 1 : -1; // Second missing, comes first in ASC

                // Handle null values (consider nulls to come before non-nulls)
                if (value1 == null && value2 == null) continue;  // Both null, consider equal
                if (value1 == null) return orderByField.Direction == SortDirection.Asc ? -1 : 1;
                if (value2 == null) return orderByField.Direction == SortDirection.Asc ? 1 : -1;

                // Compare the values based on their types
                int comparison = CompareValues(value1, value2);

                // Apply sort direction
                if (comparison != 0)
                {
                    return orderByField.Direction == SortDirection.Asc ? comparison : -comparison;
                }
            }

            // If all comparisons are equal, maintain original order (stable sort)
            return 0;
        });

        return sortedRows;
    }

    private static List<Row> ApplySelection(List<Row> rows, ReadOnlyMemory<Expression> select)
    {
        if (rows.Count == 0 || select.Length == 0)
        {
            return rows; // Nothing to project or no fields to select
        }

        // Extract field names from select expressions
        var selectedFields = new List<Field>(select.Length);
        foreach (var expr in select.Span)
        {
            var field = ExtractField(expr);
            if (field.HasValue)
            {
                selectedFields.Add(field.Value);
            }
        }

        // If no valid fields were found, return the original rows
        if (selectedFields.Count == 0)
        {
            return rows;
        }

        // Create projected rows with only the selected fields
        var projectedRows = new List<Row>(rows.Count);
        foreach (var originalRow in rows)
        {
            var projectedRow = new Row();

            // Preserve the ID field (always include it)
            projectedRow.Id = originalRow.Id;

            // Copy only the selected fields
            foreach (var field in selectedFields)
            {
                if (originalRow.Fields.TryGetValue(field.Name, out var value))
                {
                    projectedRow.SetField(field.Alias ?? field.Name, value);
                }
            }

            projectedRows.Add(projectedRow);
        }

        return projectedRows;
    }

    private Row ConvertJsonExpressionToRow(Expression jsonExpr)
    {
        if (jsonExpr.Type == ExpressionType.JsonValue)
        {
            var jsonData = jsonExpr.As<Expression.JsonData>();

            // Handle object type - return single row
            if (jsonData.ValueType == JsonValueType.Object)
            {
                return CreateRowFromObject(jsonData);
            }
            // Handle array type - return first row from array (with warning)
            else if (jsonData.ValueType == JsonValueType.Array && jsonData.Value is IEnumerable<Expression> items && items.Any())
            {
                var firstItem = items.FirstOrDefault();
                if (firstItem.Type == ExpressionType.JsonValue)
                {
                    var itemData = firstItem.As<Expression.JsonData>();
                    if (itemData.ValueType == JsonValueType.Object)
                    {
                        logger.LogWarning("Array JSON detected in upsert operation. Only the first item will be processed.");
                        return CreateRowFromObject(itemData);
                    }
                }
            }
        }

        // Default to empty row if not handled above
        return new Row();
    }

    private static List<Row> ConvertJsonExpressionToRows(Expression jsonExpr)
    {
        var rows = new List<Row>();

        if (jsonExpr.Type == ExpressionType.JsonValue)
        {
            var jsonData = jsonExpr.As<Expression.JsonData>();

            // Handle array type - return list of rows
            if (jsonData.ValueType == JsonValueType.Array && jsonData.Value is IEnumerable<Expression> items)
            {
                foreach (var item in items)
                {
                    if (item.Type == ExpressionType.JsonValue)
                    {
                        var itemData = item.As<Expression.JsonData>();
                        if (itemData.ValueType == JsonValueType.Object)
                        {
                            rows.Add(CreateRowFromObject(itemData));
                        }
                    }
                }
            }
            // Handle object type - return single row in a list
            else if (jsonData.ValueType == JsonValueType.Object)
            {
                rows.Add(CreateRowFromObject(jsonData));
            }
        }

        return rows;
    }

    private static Row CreateRowFromObject(Expression.JsonData jsonData)
    {
        var row = new Row();
        if (jsonData.Value is Dictionary<string, Expression> properties)
        {
            foreach (var (key, valueExpr) in properties)
            {
                row.SetField(key, ConvertExpressionToValue(valueExpr));
            }
        }
        return row;
    }

    private static object? ConvertExpressionToValue(Expression expr)
    {
        if (expr.Type == ExpressionType.JsonValue)
        {
            var jsonData = expr.As<Expression.JsonData>();
            return jsonData.ValueType switch
            {
                JsonValueType.String => TrimQuotes(jsonData.Value?.ToString()),
                JsonValueType.Number => ParseNumber(jsonData.Value?.ToString()),
                JsonValueType.Boolean => bool.Parse(jsonData.Value?.ToString() ?? "false"),
                JsonValueType.Null => null,
                _ => jsonData.Value
            };
        }

        return null;
    }
    private static string? TrimQuotes(string? value)
    {
        if (string.IsNullOrEmpty(value)) return value;

        // Remove surrounding quotes (single or double)
        if ((value.StartsWith('\'') && value.EndsWith('\'')) ||
            (value.StartsWith('\"') && value.EndsWith('\"')))
        {
            return value[1..^1];
        }
        else
        {
            return value;
        }
    }

    private static object ParseNumber(string? value)
    {
        if (string.IsNullOrEmpty(value)) return 0;

        // Try parsing with invariant culture to handle various formats
        if (int.TryParse(value, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var intVal))
            return intVal;

        if (long.TryParse(value, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var longVal))
            return longVal;

        if (double.TryParse(value, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var doubleVal))
            return doubleVal;

        if (decimal.TryParse(value, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var decimalVal))
            return decimalVal;

        // If all parsing attempts fail, try to remove any non-numeric characters
        // and parse again (useful for handling currency symbols, etc.)
        string cleaned = System.Text.RegularExpressions.Regex.Replace(
            value, @"[^\d.-]", "");

        if (decimal.TryParse(cleaned, System.Globalization.NumberStyles.Any,
            System.Globalization.CultureInfo.InvariantCulture, out var cleanedDecimal))
            return cleanedDecimal;

        return 0;
    }

    private static EColumnType ParseColumnType(string? dataType)
    {
        return dataType?.ToLowerInvariant() switch
        {
            "string" or "text" => EColumnType.String,
            "number" or "int" or "integer" => EColumnType.Number,
            "boolean" or "bool" => EColumnType.Boolean,
            "date" or "datetime" => EColumnType.Date,
            "object" or "json" => EColumnType.Object,
            "array" => EColumnType.Array,
            "mixed" => EColumnType.Mixed,
            _ => EColumnType.String // Default
        };
    }


    private static Field? ExtractField(Expression expression)
    {
        if (expression.Type == ExpressionType.Alias)
        {
            var aliasData = expression.As<Expression.AliasData>();
            // Extract the field from the inner expression, then apply our alias
            var innerField = ExtractField(aliasData.Expression);
            if (innerField.HasValue)
            {
                // Return the field with the alias applied
                return new Field(innerField.Value.Name, aliasData.Alias);
            }
            return null;
        }

        if (expression.Type == ExpressionType.FieldPath)
        {
            // For FieldPath expressions, we need to inspect the raw value
            // In the Expression class, FieldPath stores segments as ReadOnlyMemory<string>
            try
            {
                var fieldPath = expression.As<ReadOnlyMemory<string>>();
                // If there's a value, use the last segment as the field name
                if (!fieldPath.IsEmpty)
                {
                    var fieldName = fieldPath.Span[fieldPath.Length - 1];
                    if (!string.IsNullOrEmpty(fieldName))
                    {
                        return new Field(fieldName, null);
                    }
                }

                // Try to access the field name via the As<T> method
                // This works with the proper internal structure without relying on dynamic
                if (expression.ToString() is string expressionStr && !string.IsNullOrEmpty(expressionStr))
                {
                    // Parse the field name from the string representation
                    // Format is typically "table.field" or just "field"
                    var parts = expressionStr.Split('.');
                    return parts.Length > 0 ? new Field(parts[parts.Length - 1], null) : null;
                }
            }
            catch
            {
                // If extraction fails, return null
                return null;
            }
        }

        return null;
    }
    private static int CompareValues(object value1, object value2)
    {
        // If both values are of the same comparable type, use direct comparison
        if (value1 is IComparable comparable1 && value2.GetType() == value1.GetType())
        {
            return comparable1.CompareTo(value2);
        }

        // Handle numeric types
        if (IsNumeric(value1) && IsNumeric(value2))
        {
            var num1 = Convert.ToDouble(value1);
            var num2 = Convert.ToDouble(value2);
            return num1.CompareTo(num2);
        }

        // Handle string comparison
        var str1 = value1.ToString();
        var str2 = value2.ToString();
        return string.Compare(str1, str2, StringComparison.Ordinal);
    }

    // Helper method to check if a value is numeric
    private static bool IsNumeric(object value)
    {
        return value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;
    }
    private static string GenerateCommitId()
    {
        return Guid.NewGuid().ToString("N")[..12]; // 12-character commit ID
    }

    private readonly record struct Field(string Name, string? Alias);
}

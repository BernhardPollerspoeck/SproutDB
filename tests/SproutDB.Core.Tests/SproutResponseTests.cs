using SproutDB.Core;

namespace SproutDB.Core.Tests;

public class SproutResponseTests
{
    [Fact]
    public void DefaultResponse_HasNullableFieldsAsNull()
    {
        var response = new SproutResponse { Operation = SproutOperation.Error };

        Assert.Equal(SproutOperation.Error, response.Operation);
        Assert.Null(response.Data);
        Assert.Equal(0, response.Affected);
        Assert.Null(response.Schema);
        Assert.Null(response.Paging);
        Assert.Null(response.Errors);
        Assert.Null(response.AnnotatedQuery);
    }

    [Fact]
    public void GetResponse_WithData()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data =
            [
                new() { ["id"] = 1ul, ["name"] = "John", ["age"] = (byte)25 },
                new() { ["id"] = 2ul, ["name"] = "Jane", ["age"] = (byte)30 },
            ],
            Affected = 2,
        };

        Assert.Equal(SproutOperation.Get, response.Operation);
        Assert.NotNull(response.Data);
        Assert.Equal(2, response.Data.Count);
        Assert.Equal(2, response.Affected);
        Assert.Null(response.Schema);
        Assert.Null(response.Paging);
        Assert.Null(response.Errors);
        Assert.Null(response.AnnotatedQuery);
    }

    [Fact]
    public void UpsertResponse_WithInsertedRecord()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.Upsert,
            Data =
            [
                new()
                {
                    ["id"] = 1ul,
                    ["name"] = "John",
                    ["email"] = "john@test.com",
                    ["age"] = (byte)25,
                    ["active"] = true,
                },
            ],
            Affected = 1,
        };

        Assert.Equal(SproutOperation.Upsert, response.Operation);
        Assert.Single(response.Data!);
        Assert.Equal(1, response.Affected);
    }

    [Fact]
    public void ErrorResponse_WithErrorsAndAnnotatedQuery()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.Error,
            Errors =
            [
                new() { Code = "UNKNOWN_TABLE", Message = "Table 'userss' does not exist" },
                new() { Code = "UNKNOWN_COLUMN", Message = "Column 'agee' does not exist in table 'users'" },
            ],
            AnnotatedQuery = "get userss ##unknown table 'userss'## where agee ##unknown column 'agee'## > 18",
        };

        Assert.Equal(SproutOperation.Error, response.Operation);
        Assert.Null(response.Data);
        Assert.Equal(0, response.Affected);
        Assert.NotNull(response.Errors);
        Assert.Equal(2, response.Errors.Count);
        Assert.Equal("UNKNOWN_TABLE", response.Errors[0].Code);
        Assert.NotNull(response.AnnotatedQuery);
    }

    [Fact]
    public void CreateDatabaseResponse_WithSchema()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.CreateDatabase,
            Schema = new SchemaInfo { Database = "shop" },
        };

        Assert.Equal(SproutOperation.CreateDatabase, response.Operation);
        Assert.Null(response.Data);
        Assert.Equal(0, response.Affected);
        Assert.NotNull(response.Schema);
        Assert.Equal("shop", response.Schema.Database);
    }

    [Fact]
    public void CreateTableResponse_WithSchemaAndColumns()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.CreateTable,
            Schema = new SchemaInfo
            {
                Table = "users",
                Columns =
                [
                    new() { Name = "id", Type = "ulong", Nullable = false, Strict = true, Auto = true },
                    new() { Name = "name", Type = "string", Size = 255, Nullable = true, Strict = false },
                    new() { Name = "email", Type = "string", Size = 320, Nullable = true, Strict = true },
                    new() { Name = "active", Type = "bool", Nullable = false, Default = "true", Strict = false },
                ],
            },
        };

        Assert.Equal(SproutOperation.CreateTable, response.Operation);
        Assert.NotNull(response.Schema);
        Assert.Equal("users", response.Schema.Table);
        Assert.NotNull(response.Schema.Columns);
        Assert.Equal(4, response.Schema.Columns.Count);

        var idCol = response.Schema.Columns[0];
        Assert.Equal("id", idCol.Name);
        Assert.Equal("ulong", idCol.Type);
        Assert.False(idCol.Nullable);
        Assert.True(idCol.Strict);
        Assert.True(idCol.Auto);

        var activeCol = response.Schema.Columns[3];
        Assert.Equal("active", activeCol.Name);
        Assert.False(activeCol.Nullable);
        Assert.Equal("true", activeCol.Default);
    }

    [Fact]
    public void DescribeAllTablesResponse()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.Describe,
            Schema = new SchemaInfo
            {
                Tables = ["users", "orders", "products"],
            },
        };

        Assert.NotNull(response.Schema);
        Assert.NotNull(response.Schema.Tables);
        Assert.Equal(3, response.Schema.Tables.Count);
        Assert.Null(response.Schema.Table);
    }

    [Fact]
    public void GetResponse_WithPaging()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = [new() { ["id"] = 1ul }],
            Affected = 100,
            Paging = new PagingInfo
            {
                Total = 1523,
                PageSize = 100,
                Page = 1,
                Next = "get users where active = true page 2 size 100",
            },
        };

        Assert.NotNull(response.Paging);
        Assert.Equal(1523, response.Paging.Total);
        Assert.Equal(100, response.Paging.PageSize);
        Assert.Equal(1, response.Paging.Page);
        Assert.NotNull(response.Paging.Next);
    }

    [Fact]
    public void GetResponse_LastPage_NoNext()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data = [new() { ["id"] = 1ul }],
            Affected = 23,
            Paging = new PagingInfo
            {
                Total = 123,
                PageSize = 100,
                Page = 2,
                Next = null,
            },
        };

        Assert.NotNull(response.Paging);
        Assert.Null(response.Paging.Next);
    }

    [Fact]
    public void IndexResponse_WithSchemaInfo()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.CreateIndex,
            Schema = new SchemaInfo
            {
                Table = "users",
                Column = "email",
                Index = "users.email",
            },
        };

        Assert.NotNull(response.Schema);
        Assert.Equal("users", response.Schema.Table);
        Assert.Equal("email", response.Schema.Column);
        Assert.Equal("users.email", response.Schema.Index);
    }

    [Fact]
    public void DataRecords_SupportNullValues()
    {
        var response = new SproutResponse
        {
            Operation = SproutOperation.Get,
            Data =
            [
                new() { ["id"] = 1ul, ["name"] = "John", ["email"] = null },
            ],
            Affected = 1,
        };

        Assert.Single(response.Data!);
        Assert.Null(response.Data[0]["email"]);
    }
}

using SproutDB.Core.Admin.Components.Pages;

namespace SproutDB.Core.Tests;

public class CollapseQueryTests
{
    [Fact]
    public void SingleLine_Unchanged()
    {
        var result = QueryPage.CollapseQuery("get users");
        Assert.Equal("get users", result);
    }

    [Fact]
    public void FormattedGet_CollapsedToOneLine()
    {
        var input = "get users\n    where active = true\n    order by name asc";
        var result = QueryPage.CollapseQuery(input);
        Assert.Equal("get users where active = true order by name asc", result);
    }

    [Fact]
    public void FormattedFollowQuery_CollapsedToOneLine()
    {
        var input = "get customers select name, city\n"
                  + "    where city = 'München'\n"
                  + "    order by name asc\n"
                  + "    page 1 size 15\n"
                  + "    follow customers._id -> orders.customer_id as orders\n"
                  + "        select status, total\n"
                  + "        where status = 'delivered'\n"
                  + "    follow orders._id -> order_items.order_id as items\n"
                  + "        select quantity, unit_price";

        var result = QueryPage.CollapseQuery(input);
        Assert.Contains("follow customers._id", result);
        Assert.Contains("select status, total", result);
        Assert.Contains("follow orders._id", result);
        Assert.Contains("select quantity, unit_price", result);
        Assert.DoesNotContain("\n", result);
    }

    [Fact]
    public void EmptyLines_Skipped()
    {
        var result = QueryPage.CollapseQuery("get users\n\n    where active = true");
        Assert.Equal("get users where active = true", result);
    }

    [Fact]
    public void CommentLines_Skipped()
    {
        var result = QueryPage.CollapseQuery("##comment##\nget users");
        Assert.Equal("get users", result);
    }

    [Fact]
    public void TabIndented_Collapsed()
    {
        var result = QueryPage.CollapseQuery("get users\n\twhere active = true");
        Assert.Equal("get users where active = true", result);
    }

    [Fact]
    public void Empty_ReturnsEmpty()
    {
        Assert.Equal("", QueryPage.CollapseQuery(""));
        Assert.Equal("", QueryPage.CollapseQuery("   "));
        Assert.Equal("", QueryPage.CollapseQuery("\n\n"));
    }
}

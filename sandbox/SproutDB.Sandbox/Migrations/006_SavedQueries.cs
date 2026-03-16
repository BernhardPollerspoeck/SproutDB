using SproutDB.Core;

namespace SproutDB.Sandbox.Migrations;

public sealed class SavedQueries : IMigration
{
    public int Order => 6;

    public void Up(ISproutDatabase db)
    {
        // ── Pinned (daily ops) ──────────────────────────────
        db.SaveQuery("Low Stock Alert", "get plants where stock < 20 order by stock", pinned: true);
        db.SaveQuery("Pending Orders", "get orders where status = 'pending' order by order_date", pinned: true);

        // ── Browse ──────────────────────────────────────────
        db.SaveQuery("All Plants", "get plants order by category asc, name");
        db.SaveQuery("All Customers", "get customers order by city asc, joined_at desc");
        db.SaveQuery("All Orders", "get orders order by order_date desc");

        // ── Where: comparison + logic ───────────────────────
        db.SaveQuery("Budget Plants", "get plants where price <= 3 order by name");
        db.SaveQuery("Premium Trees", "get plants where category = 'tree' and price >= 40 order by price desc");
        db.SaveQuery("Complex Filter", "get plants where not (category = 'herb' or category = 'vegetable') and stock > 10 and price < 50 order by price desc");

        // ── Where: string operators ─────────────────────────
        db.SaveQuery("Search Rose", "get plants where name contains 'Rose' order by name");
        db.SaveQuery("March Plantings", "get plants where planted_at starts '2025-03' order by planted_at");
        db.SaveQuery("Email .de Domain", "get customers where email ends '.de' order by name");

        // ── Where: between / in / null ──────────────────────
        db.SaveQuery("Mid-Range Plants", "get plants where price between 5 and 15 order by price");
        db.SaveQuery("Extreme Prices", "get plants where price not between 3 and 20 order by price desc");
        db.SaveQuery("Fruits & Flowers", "get plants where category in ['fruit', 'flower'] order by category, name");
        db.SaveQuery("Not Herbs Or Veg", "get plants where category not in ['herb', 'vegetable'] order by name");
        db.SaveQuery("Plants Without Tags", "get plants where tags is null order by name");
        db.SaveQuery("Plants With Tags", "get plants where tags is not null order by name");

        // ── Select / -select / distinct ─────────────────────
        db.SaveQuery("Plant Cards", "get plants select name, species, category, price order by name");
        db.SaveQuery("Plants Compact", "get plants -select _id, supplier_id, planted_at order by name");
        db.SaveQuery("All Categories", "get plants select category distinct");
        db.SaveQuery("All Cities", "get customers select city distinct");

        // ── Count / limit / page ────────────────────────────
        db.SaveQuery("Total Plants", "get plants count");
        db.SaveQuery("Plants Per Category", "get plants count group by category");
        db.SaveQuery("Cheapest 5", "get plants order by price limit 5");
        db.SaveQuery("Page 2 Plants", "get plants order by name page 2 size 10");

        // ── Aggregates: sum / avg / min / max ───────────────
        db.SaveQuery("Revenue By Status", "get orders sum total as revenue group by status");
        db.SaveQuery("Avg Price Per Category", "get plants avg price as avg_price group by category");
        db.SaveQuery("Total Revenue", "get orders sum total as total_revenue where status = 'delivered'");
        db.SaveQuery("Stock Overview", "get plants sum stock as total, min stock as lowest, max stock as highest, avg stock as average");

        // ── Computed columns ────────────────────────────────
        db.SaveQuery("Line Totals", "get order_items select quantity, unit_price, quantity * unit_price as line_total order by line_total desc");
        db.SaveQuery("Stock Value", "get plants select name, price, stock, price * stock as value order by value desc");

        // ── Follow: inner join ──────────────────────────────
        db.SaveQuery("Orders With Customer",
            "get orders follow orders.customer_id -> customers._id as c select c.name, order_date, status, total order by order_date desc");
        db.SaveQuery("Plants With Supplier",
            "get plants follow plants.supplier_id -> suppliers._id as s select name, category, price, s.name, s.country order by name");

        // ── Follow: left join (->?) ─────────────────────────
        db.SaveQuery("All Customers + Orders",
            "get customers follow customers._id ->? orders.customer_id as ord select name, city, ord.order_date, ord.total order by name");

        // ── Follow: with where on followed table ────────────
        db.SaveQuery("Big Spender Orders",
            "get customers follow customers._id -> orders.customer_id as ord where ord.total >= 80 select name, ord.order_date, ord.total order by ord.total desc");

        // ── Follow: chained 3-table join ────────────────────
        db.SaveQuery("Full Order Details",
            "get order_items follow order_items.order_id -> orders._id as ord follow order_items.plant_id -> plants._id as plant select ord.order_date, plant.name, quantity, unit_price order by ord.order_date desc");

        // ── Follow: chained + computed + where ──────────────
        db.SaveQuery("Order Item Values",
            "get order_items follow order_items.plant_id -> plants._id as plant follow order_items.order_id -> orders._id as ord select plant.name, quantity, unit_price, quantity * unit_price as total, ord.status order by total desc");

        // ── ** The Big One ** ───────────────────────────────
        db.SaveQuery("** Full Receipt",
            "get customers follow customers._id -> orders.customer_id as ord where ord.status = 'delivered' follow ord._id -> order_items.order_id as item follow item.plant_id -> plants._id as plant select name, ord.order_date, plant.name, plant.category, item.quantity, item.unit_price, item.quantity * item.unit_price as line_total order by name, ord.order_date desc limit 50");

        db.SaveQuery("** Trees & Fruits Delivered",
            "get order_items follow order_items.order_id -> orders._id as ord where ord.status = 'delivered' follow order_items.plant_id -> plants._id as plant where plant.category in ['tree', 'fruit'] select plant.name, plant.category, quantity, unit_price, quantity * unit_price as line_total order by line_total desc");

        db.SaveQuery("** Supplier Ratings",
            "get suppliers avg rating as avg_rating group by country order by avg_rating desc");
    }
}

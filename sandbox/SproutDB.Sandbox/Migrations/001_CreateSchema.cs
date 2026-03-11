using SproutDB.Core;

namespace SproutDB.Sandbox.Migrations;

public sealed class CreateSchema : IMigration
{
    public int Order => 1;

    public void Up(ISproutDatabase db)
    {
        db.Query("create table suppliers (name string 200, country string 100, contact_email string 320, rating sint)");
        db.Query("create table plants (name string 200, species string 200, category string 50, price sint, stock sint, supplier_id string 36, planted_at string 30)");
        db.Query("create table customers (name string 200, email string 320, city string 100, joined_at string 30)");
        db.Query("create table orders (customer_id string 36, order_date string 30, status string 20, total sint)");
        db.Query("create table order_items (order_id string 36, plant_id string 36, quantity sint, unit_price sint)");
        db.Query("create table carts (customer_id string 36, plant_id string 36, quantity sint, note string 200) ttl 5m");
    }
}

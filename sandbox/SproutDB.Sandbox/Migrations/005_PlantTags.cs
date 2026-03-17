using SproutDB.Core;

namespace SproutDB.Sandbox.Migrations;

public sealed class PlantTags : IMigration
{
    public int Order => 5;

    public void Up(ISproutDatabase db)
    {
        db.Query("add column plants.tags array string 30");

        // Tag plants by category + properties
        var plants = db.Query("get plants select _id, name, category, price")[0];
        if (plants.Data is null) return;

        foreach (var row in plants.Data)
        {
            var id = row["_id"]?.ToString();
            var name = row["name"]?.ToString() ?? "";
            var category = row["category"]?.ToString() ?? "";
            var price = int.TryParse(row["price"]?.ToString(), out var p) ? p : 0;
            if (id is null) continue;

            var tags = new List<string>();

            // Category-based tags
            switch (category)
            {
                case "herb":
                    tags.Add("kitchen");
                    tags.Add("aromatic");
                    if (name is "Basilikum" or "Petersilie" or "Schnittlauch" or "Dill")
                        tags.Add("annual");
                    else
                        tags.Add("perennial");
                    break;
                case "flower":
                    tags.Add("decorative");
                    if (name.StartsWith("Rose"))
                        tags.Add("fragrant");
                    if (name.StartsWith("Tulpe"))
                        tags.Add("spring");
                    break;
                case "vegetable":
                    tags.Add("kitchen");
                    tags.Add("edible");
                    if (name is "Tomate" or "Paprika" or "Gurke" or "Zucchini")
                        tags.Add("summer");
                    break;
                case "tree":
                    tags.Add("perennial");
                    tags.Add("outdoor");
                    if (price >= 40)
                        tags.Add("premium");
                    break;
                case "fruit":
                    tags.Add("edible");
                    tags.Add("perennial");
                    if (name is "Erdbeere" or "Himbeere" or "Blaubeere" or "Brombeere")
                        tags.Add("berry");
                    break;
            }

            // Price-based tags
            if (price <= 3)
                tags.Add("budget");
            else if (price >= 20)
                tags.Add("premium");

            if (tags.Count == 0) continue;

            var arrayLiteral = "[" + string.Join(", ", tags.Select(t => $"'{t}'")) + "]";
            db.Query($"upsert plants {{_id: {id}, tags: {arrayLiteral}}}");
        }
    }
}

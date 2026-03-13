using SproutDB.Core;

namespace SproutDB.Sandbox.Migrations;

public sealed class PlantImages : IMigration
{
    public int Order => 4;

    public void Up(ISproutDatabase db)
    {
        db.Query("add column plants.image blob");

        // Map species → Wikipedia page title for thumbnail lookup
        var speciesPages = new Dictionary<string, string>
        {
            ["Ocimum basilicum"] = "Basil",
            ["Salvia rosmarinus"] = "Rosemary",
            ["Lavandula angustifolia"] = "Lavandula_angustifolia",
            ["Mentha spicata"] = "Spearmint",
            ["Helianthus annuus"] = "Common_sunflower",
            ["Rosa gallica"] = "Rosa_gallica",
            ["Tulipa gesneriana"] = "Tulipa_gesneriana",
            ["Hydrangea macrophylla"] = "Hydrangea_macrophylla",
            ["Solanum lycopersicum"] = "Tomato",
            ["Capsicum annuum"] = "Capsicum_annuum",
            ["Cucumis sativus"] = "Cucumber",
            ["Daucus carota"] = "Carrot",
            ["Malus domestica"] = "Apple",
            ["Prunus avium"] = "Prunus_avium",
            ["Citrus limon"] = "Lemon",
            ["Olea europaea"] = "Olive_tree",
            ["Fragaria ananassa"] = "Strawberry",
            ["Rubus idaeus"] = "Rubus_idaeus",
            ["Vaccinium corymbosum"] = "Blueberry",
            ["Vitis vinifera"] = "Vitis_vinifera",
        };

        using var http = new HttpClient();
        http.DefaultRequestHeaders.Add("User-Agent", "SproutDB-Sandbox/1.0");
        http.Timeout = TimeSpan.FromSeconds(5);

        // Get all plants
        var plants = db.Query("get plants select _id, species");
        if (plants.Data is null) return;

        foreach (var row in plants.Data)
        {
            var id = row["_id"]?.ToString();
            var species = row["species"]?.ToString();
            if (id is null || species is null) continue;
            if (!speciesPages.TryGetValue(species, out var page)) continue;

            try
            {
                var thumbUrl = GetWikipediaThumbnail(http, page);
                if (thumbUrl is null) continue;

                var imageBytes = http.GetByteArrayAsync(thumbUrl).GetAwaiter().GetResult();
                var base64 = Convert.ToBase64String(imageBytes);
                db.Query($"upsert plants {{ _id: {id}, image: '{base64}' }}");
            }
            catch
            {
                // Skip plants where image download fails
            }
        }
    }

    private static string? GetWikipediaThumbnail(HttpClient http, string pageTitle)
    {
        var url = $"https://en.wikipedia.org/api/rest_v1/page/summary/{Uri.EscapeDataString(pageTitle)}";
        try
        {
            var json = http.GetStringAsync(url).GetAwaiter().GetResult();
            // Simple JSON parsing — find "source":"..." in thumbnail object
            var thumbIdx = json.IndexOf("\"thumbnail\"", StringComparison.Ordinal);
            if (thumbIdx < 0) return null;

            var sourceIdx = json.IndexOf("\"source\"", thumbIdx, StringComparison.Ordinal);
            if (sourceIdx < 0) return null;

            var colonIdx = json.IndexOf(':', sourceIdx + 8);
            if (colonIdx < 0) return null;

            var quoteStart = json.IndexOf('"', colonIdx + 1);
            if (quoteStart < 0) return null;

            var quoteEnd = json.IndexOf('"', quoteStart + 1);
            if (quoteEnd < 0) return null;

            return json.Substring(quoteStart + 1, quoteEnd - quoteStart - 1);
        }
        catch
        {
            return null;
        }
    }
}

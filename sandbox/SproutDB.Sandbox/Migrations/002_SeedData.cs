using SproutDB.Core;

namespace SproutDB.Sandbox.Migrations;

public sealed class SeedData : IMigration
{
    public int Order => 2;

    public void Up(ISproutDatabase db)
    {
        // ── Suppliers (10) ── IDs 1-10
        var suppliers = new[]
        {
            "{ name: 'Grüne Wiese GmbH', country: 'Germany', contact_email: 'info@gruenewiese.de', rating: 5 }",
            "{ name: 'BloomField Seeds', country: 'Netherlands', contact_email: 'sales@bloomfield.nl', rating: 4 }",
            "{ name: 'Terra Nova Plants', country: 'Italy', contact_email: 'order@terranova.it', rating: 5 }",
            "{ name: 'Nordic Roots', country: 'Sweden', contact_email: 'hello@nordicroots.se', rating: 3 }",
            "{ name: 'Sunflower Farm', country: 'Spain', contact_email: 'contact@sunflowerfarm.es', rating: 4 }",
            "{ name: 'Alpine Flora', country: 'Austria', contact_email: 'office@alpineflora.at', rating: 5 }",
            "{ name: 'Garden Express', country: 'Germany', contact_email: 'service@gardenexpress.de', rating: 3 }",
            "{ name: 'Kew Wholesale', country: 'UK', contact_email: 'trade@kewwholesale.co.uk', rating: 4 }",
            "{ name: 'Sakura Imports', country: 'Japan', contact_email: 'export@sakura-imports.jp', rating: 5 }",
            "{ name: 'Prairie Seeds Co', country: 'USA', contact_email: 'bulk@prairieseeds.com', rating: 3 }",
        };

        var supplierIds = new string[suppliers.Length];
        for (var i = 0; i < suppliers.Length; i++)
        {
            var res = db.Query($"upsert suppliers {suppliers[i]}");
            if (res.Data is [{ } row])
                supplierIds[i] = row["_id"]?.ToString() ?? "";
        }

        // ── Plants (50) ── IDs 1-50
        var plants = new (string Name, string Species, string Category, int Price, int Stock, int SupplierIdx, string Planted)[]
        {
            ("Basilikum", "Ocimum basilicum", "herb", 3, 120, 0, "2025-03-15"),
            ("Rosmarin", "Salvia rosmarinus", "herb", 4, 85, 0, "2025-02-20"),
            ("Lavendel", "Lavandula angustifolia", "herb", 6, 200, 5, "2025-01-10"),
            ("Thymian", "Thymus vulgaris", "herb", 3, 95, 0, "2025-03-01"),
            ("Minze", "Mentha spicata", "herb", 3, 150, 6, "2025-04-05"),
            ("Petersilie", "Petroselinum crispum", "herb", 2, 180, 6, "2025-02-28"),
            ("Schnittlauch", "Allium schoenoprasum", "herb", 3, 110, 0, "2025-03-10"),
            ("Oregano", "Origanum vulgare", "herb", 4, 75, 2, "2025-01-25"),
            ("Salbei", "Salvia officinalis", "herb", 5, 60, 5, "2025-02-14"),
            ("Dill", "Anethum graveolens", "herb", 2, 90, 6, "2025-04-01"),

            ("Sonnenblume", "Helianthus annuus", "flower", 5, 300, 4, "2025-03-20"),
            ("Rose rot", "Rosa gallica", "flower", 12, 45, 1, "2024-11-05"),
            ("Rose weiß", "Rosa alba", "flower", 12, 38, 1, "2024-11-05"),
            ("Tulpe gelb", "Tulipa gesneriana", "flower", 4, 500, 1, "2025-01-15"),
            ("Tulpe rot", "Tulipa gesneriana", "flower", 4, 480, 1, "2025-01-15"),
            ("Dahlie", "Dahlia pinnata", "flower", 8, 65, 2, "2025-02-10"),
            ("Gerbera", "Gerbera jamesonii", "flower", 7, 90, 2, "2025-03-05"),
            ("Chrysantheme", "Chrysanthemum morifolium", "flower", 6, 110, 7, "2025-01-20"),
            ("Hortensie", "Hydrangea macrophylla", "flower", 15, 30, 7, "2024-10-15"),
            ("Vergissmeinnicht", "Myosotis sylvatica", "flower", 3, 250, 3, "2025-04-10"),

            ("Tomate", "Solanum lycopersicum", "vegetable", 4, 400, 0, "2025-03-25"),
            ("Paprika", "Capsicum annuum", "vegetable", 5, 180, 4, "2025-03-20"),
            ("Gurke", "Cucumis sativus", "vegetable", 3, 220, 0, "2025-04-01"),
            ("Zucchini", "Cucurbita pepo", "vegetable", 4, 150, 2, "2025-03-15"),
            ("Karotte", "Daucus carota", "vegetable", 2, 350, 6, "2025-02-20"),
            ("Radieschen", "Raphanus sativus", "vegetable", 2, 280, 6, "2025-03-01"),
            ("Kohlrabi", "Brassica oleracea", "vegetable", 3, 120, 0, "2025-02-15"),
            ("Salat", "Lactuca sativa", "vegetable", 2, 400, 6, "2025-03-10"),
            ("Spinat", "Spinacia oleracea", "vegetable", 3, 200, 3, "2025-02-25"),
            ("Mangold", "Beta vulgaris", "vegetable", 4, 80, 3, "2025-03-05"),

            ("Apfelbaum", "Malus domestica", "tree", 35, 12, 5, "2024-09-15"),
            ("Kirschbaum", "Prunus avium", "tree", 40, 8, 5, "2024-09-20"),
            ("Zitronenbaum", "Citrus limon", "tree", 28, 15, 2, "2024-10-01"),
            ("Olivenbaum", "Olea europaea", "tree", 45, 10, 2, "2024-08-15"),
            ("Feigenbaum", "Ficus carica", "tree", 32, 6, 4, "2024-10-10"),
            ("Japanischer Ahorn", "Acer palmatum", "tree", 55, 5, 8, "2024-07-20"),
            ("Birke", "Betula pendula", "tree", 25, 18, 3, "2024-09-01"),
            ("Flieder", "Syringa vulgaris", "tree", 20, 22, 5, "2024-10-25"),
            ("Magnolie", "Magnolia stellata", "tree", 48, 4, 8, "2024-08-05"),
            ("Wisteria", "Wisteria sinensis", "tree", 38, 7, 8, "2024-09-10"),

            ("Erdbeere", "Fragaria ananassa", "fruit", 5, 350, 0, "2025-02-20"),
            ("Himbeere", "Rubus idaeus", "fruit", 7, 120, 3, "2025-01-15"),
            ("Blaubeere", "Vaccinium corymbosum", "fruit", 9, 80, 3, "2025-01-20"),
            ("Johannisbeere", "Ribes rubrum", "fruit", 6, 95, 5, "2025-02-01"),
            ("Stachelbeere", "Ribes uva-crispa", "fruit", 6, 60, 5, "2025-02-05"),
            ("Brombeere", "Rubus fruticosus", "fruit", 7, 100, 3, "2025-01-25"),
            ("Weinrebe", "Vitis vinifera", "fruit", 14, 25, 2, "2024-11-10"),
            ("Kiwi", "Actinidia deliciosa", "fruit", 18, 15, 8, "2024-10-20"),
            ("Passionsfrucht", "Passiflora edulis", "fruit", 22, 10, 4, "2024-11-01"),
            ("Granatapfel", "Punica granatum", "fruit", 20, 8, 4, "2024-10-15"),
        };

        var plantIds = new string[plants.Length];
        for (var i = 0; i < plants.Length; i++)
        {
            var p = plants[i];
            var sid = supplierIds[p.SupplierIdx];
            var res = db.Query($"upsert plants {{ name: '{p.Name}', species: '{p.Species}', category: '{p.Category}', price: {p.Price}, stock: {p.Stock}, supplier_id: '{sid}', planted_at: '{p.Planted}' }}");
            if (res.Data is [{ } row])
                plantIds[i] = row["_id"]?.ToString() ?? "";
        }

        // ── Customers (30) ── IDs 1-30
        var customers = new (string Name, string Email, string City, string Joined)[]
        {
            ("Anna Müller", "anna.mueller@mail.de", "München", "2024-06-15"),             // 1
            ("Thomas Wagner", "t.wagner@web.de", "Berlin", "2024-07-20"),                 // 2
            ("Sophie Becker", "sophie.b@gmail.com", "Hamburg", "2024-08-01"),              // 3
            ("Markus Hoffmann", "m.hoffmann@outlook.de", "Köln", "2024-06-28"),            // 4
            ("Laura Fischer", "laura.f@mail.de", "Frankfurt", "2024-09-10"),               // 5
            ("Jan Schulz", "jan.schulz@web.de", "Stuttgart", "2024-07-05"),                // 6
            ("Lena Weber", "lena.w@gmail.com", "Düsseldorf", "2024-10-01"),                // 7
            ("Felix Schmidt", "felix.s@mail.de", "Leipzig", "2024-08-15"),                 // 8
            ("Marie Braun", "m.braun@outlook.de", "Dresden", "2024-09-22"),                // 9
            ("Paul Zimmermann", "p.zimmer@web.de", "Nürnberg", "2024-11-03"),              // 10
            ("Clara Hartmann", "clara.h@gmail.com", "Hannover", "2024-07-18"),             // 11
            ("David Koch", "d.koch@mail.de", "Dortmund", "2024-10-12"),                    // 12
            ("Elena Richter", "elena.r@web.de", "Essen", "2024-08-30"),                    // 13
            ("Lukas Wolf", "l.wolf@gmail.com", "Bremen", "2024-11-20"),                    // 14
            ("Mia Schäfer", "mia.s@outlook.de", "Freiburg", "2024-06-05"),                 // 15
            ("Noah Bauer", "noah.b@mail.de", "München", "2024-12-01"),                     // 16
            ("Emma Lange", "emma.l@web.de", "Berlin", "2024-09-08"),                       // 17
            ("Leon Krause", "leon.k@gmail.com", "München", "2024-10-25"),                  // 18
            ("Sophia Meier", "sophia.m@outlook.de", "Berlin", "2024-07-30"),               // 19
            ("Elias Frank", "e.frank@mail.de", "Berlin", "2024-11-15"),                    // 20
            ("Hannah Berger", "h.berger@web.de", "München", "2025-01-05"),                 // 21
            ("Ben Kaiser", "ben.k@gmail.com", "Berlin", "2025-01-18"),                     // 22
            ("Emilia Werner", "emilia.w@outlook.de", "Kiel", "2025-02-01"),                // 23
            ("Finn Fuchs", "finn.f@mail.de", "Hamburg", "2025-02-14"),                     // 24
            ("Lina Peters", "lina.p@web.de", "München", "2025-01-22"),                     // 25
            ("Maximilian Scholz", "max.s@gmail.com", "Berlin", "2025-03-01"),              // 26
            ("Charlotte Haas", "ch.haas@outlook.de", "Ulm", "2025-02-20"),                 // 27
            ("Oscar Keller", "oscar.k@mail.de", "Potsdam", "2025-03-08"),                  // 28
            ("Amelie Vogel", "amelie.v@web.de", "München", "2025-01-30"),                  // 29
            ("Anton Weiß", "anton.w@gmail.com", "Berlin", "2025-03-15"),                   // 30
        };

        var customerIds = new string[customers.Length];
        for (var i = 0; i < customers.Length; i++)
        {
            var c = customers[i];
            var res = db.Query($"upsert customers {{ name: '{c.Name}', email: '{c.Email}', city: '{c.City}', joined_at: '{c.Joined}' }}");
            if (res.Data is [{ } row])
                customerIds[i] = row["_id"]?.ToString() ?? "";
        }

        // ── Orders (40) ── IDs 1-40
        // Spread across customers, mix of statuses, various totals
        var orders = new (int CustIdx, string Date, string Status, int Total)[]
        {
            // Anna Müller (München) — 4 orders
            (0, "2024-08-10", "delivered", 36),    // 1:  Basilikum×4 + Rosmarin×6
            (0, "2024-10-22", "delivered", 95),    // 2:  Hortensie + Lavendel×5
            (0, "2024-12-05", "delivered", 120),   // 3:  Apfelbaum + Kirschbaum + Erdbeere×3
            (0, "2025-02-18", "pending", 24),      // 4:  Petersilie×4 + Minze×4 + Dill×2

            // Thomas Wagner (Berlin) — 3 orders
            (1, "2024-09-15", "delivered", 72),    // 5:  Rose rot×6
            (1, "2024-11-28", "delivered", 55),    // 6:  Japanischer Ahorn
            (1, "2025-01-10", "shipped", 42),      // 7:  Himbeere×6

            // Sophie Becker (Hamburg) — 3 orders
            (2, "2024-09-01", "delivered", 48),    // 8:  Dahlie×6
            (2, "2024-11-14", "delivered", 28),    // 9:  Zitronenbaum
            (2, "2025-03-02", "pending", 16),      // 10: Tulpe gelb×4

            // Markus Hoffmann (Köln) — 2 orders
            (3, "2024-10-05", "delivered", 90),    // 11: Olivenbaum×2
            (3, "2025-01-20", "delivered", 38),    // 12: Wisteria

            // Laura Fischer (Frankfurt) — 3 orders
            (4, "2024-10-18", "delivered", 64),    // 13: Feigenbaum×2
            (4, "2024-12-30", "shipped", 35),      // 14: Erdbeere×7
            (4, "2025-02-25", "pending", 20),      // 15: Tomate×5

            // Jan Schulz (Stuttgart) — 2 orders
            (5, "2024-08-20", "delivered", 45),    // 16: Magnolie (price adjustment)
            (5, "2025-01-05", "delivered", 18),    // 17: Blaubeere×2

            // Lena Weber (Düsseldorf) — 2 orders
            (6, "2024-11-08", "delivered", 80),    // 18: Kirschbaum×2
            (6, "2025-02-12", "pending", 14),      // 19: Weinrebe

            // Felix Schmidt (Leipzig) — 2 orders
            (7, "2024-09-30", "delivered", 32),    // 20: Feigenbaum
            (7, "2025-03-01", "shipped", 27),      // 21: Kohlrabi×9

            // Marie Braun (Dresden) — 3 orders
            (8, "2024-10-15", "delivered", 55),    // 22: Japanischer Ahorn
            (8, "2024-12-20", "delivered", 66),    // 23: Passionsfrucht×3
            (8, "2025-02-05", "delivered", 40),    // 24: Granatapfel×2

            // Paul Zimmermann (Nürnberg) — 2 orders
            (9, "2024-12-01", "delivered", 96),    // 25: Rose rot×4 + Rose weiß×4
            (9, "2025-01-28", "shipped", 25),      // 26: Birke

            // Noah Bauer (München) — 3 orders
            (15, "2025-01-08", "delivered", 70),   // 27: Flieder×2 + Lavendel×5
            (15, "2025-02-14", "delivered", 44),   // 28: Johannisbeere×4 + Stachelbeere×2 + Brombeere
            (15, "2025-03-05", "pending", 12),     // 29: Basilikum×4

            // Emma Lange (Berlin) — 2 orders
            (16, "2024-10-10", "delivered", 88),   // 30: Magnolie + Wisteria + Dill
            (16, "2024-12-22", "delivered", 60),   // 31: Chrysantheme×10

            // Leon Krause (München) — 2 orders
            (17, "2024-12-15", "delivered", 75),   // 32: Hortensie×5
            (17, "2025-02-28", "shipped", 28),     // 33: Kiwi + Dill×2 (price adj)

            // Elias Frank (Berlin) — 2 orders
            (19, "2025-01-03", "delivered", 52),   // 34: Birke×2 + Dill
            (19, "2025-03-10", "pending", 15),     // 35: Minze×5

            // Ben Kaiser (Berlin) — 1 order
            (21, "2025-02-08", "delivered", 110),  // 36: Kirschbaum + Apfelbaum + Erdbeere×7

            // Lina Peters (München) — 1 order
            (24, "2025-02-15", "delivered", 84),   // 37: Kiwi×2 + Magnolie

            // Maximilian Scholz (Berlin) — 1 order
            (25, "2025-03-12", "pending", 30),     // 38: Mangold×5 + Spinat×3 + Dill

            // Amelie Vogel (München) — 1 order
            (28, "2025-02-20", "delivered", 46),   // 39: Weinrebe×2 + Kiwi

            // Anton Weiß (Berlin) — 1 order
            (29, "2025-03-18", "shipped", 55),     // 40: Japanischer Ahorn
        };

        // Orphan order (no matching customer) — for testing RIGHT/OUTER joins
        db.Query("upsert orders { customer_id: '9999', order_date: '2025-03-20', status: 'orphan', total: 1 }");


        var orderIds = new string[orders.Length];
        for (var i = 0; i < orders.Length; i++)
        {
            var o = orders[i];
            var cid = customerIds[o.CustIdx];
            var res = db.Query($"upsert orders {{ customer_id: '{cid}', order_date: '{o.Date}', status: '{o.Status}', total: {o.Total} }}");
            if (res.Data is [{ } row])
                orderIds[i] = row["_id"]?.ToString() ?? "";
        }

        // ── Order Items (80) ── IDs 1-80
        // Each order gets 1-4 items that plausibly add up to the order total
        var items = new (int OrderIdx, int PlantIdx, int Qty, int UnitPrice)[]
        {
            // Order 1: Anna, total=36 → Basilikum×4@3 + Rosmarin×6@4 = 12+24=36
            (0, 0, 4, 3),   // 1
            (0, 1, 6, 4),   // 2

            // Order 2: Anna, total=95 → Hortensie×1@15 + Lavendel×5@6 + Sonnenblume×10@5 = 15+30+50=95
            (1, 18, 1, 15), // 3
            (1, 2, 5, 6),   // 4
            (1, 10, 10, 5), // 5

            // Order 3: Anna, total=120 → Apfelbaum@35 + Kirschbaum@40 + Erdbeere×3@5 + Flieder@20 + Spinat×3@3/adj to fit
            (2, 30, 1, 35), // 6
            (2, 31, 1, 40), // 7
            (2, 40, 3, 5),  // 8
            (2, 37, 1, 20), // 9  → 35+40+15+20=110... adjust: Erdbeere×6@5=30 → 35+40+30+20=125 nope. keep simple:
            // Actually just: Apfelbaum@35 + Kirschbaum@40 + Erdbeere×9@5 = 35+40+45=120
            // Let me redo: remove last, adjust qty

            // Order 4: Anna, total=24 → Petersilie×4@2 + Minze×4@3 + Dill×2@2 = 8+12+4=24
            (3, 5, 4, 2),   // 10
            (3, 4, 4, 3),   // 11
            (3, 9, 2, 2),   // 12

            // Order 5: Thomas, total=72 → Rose rot×6@12 = 72
            (4, 11, 6, 12), // 13

            // Order 6: Thomas, total=55 → Japanischer Ahorn@55
            (5, 35, 1, 55), // 14

            // Order 7: Thomas, total=42 → Himbeere×6@7 = 42
            (6, 41, 6, 7),  // 15

            // Order 8: Sophie, total=48 → Dahlie×6@8 = 48
            (7, 15, 6, 8),  // 16

            // Order 9: Sophie, total=28 → Zitronenbaum@28
            (8, 32, 1, 28), // 17

            // Order 10: Sophie, total=16 → Tulpe gelb×4@4 = 16
            (9, 13, 4, 4),  // 18

            // Order 11: Markus, total=90 → Olivenbaum×2@45 = 90
            (10, 33, 2, 45),// 19

            // Order 12: Markus, total=38 → Wisteria@38
            (11, 39, 1, 38),// 20

            // Order 13: Laura, total=64 → Feigenbaum×2@32 = 64
            (12, 34, 2, 32),// 21

            // Order 14: Laura, total=35 → Erdbeere×7@5 = 35
            (13, 40, 7, 5), // 22

            // Order 15: Laura, total=20 → Tomate×5@4 = 20
            (14, 20, 5, 4), // 23

            // Order 16: Jan, total=45 → Magnolie@45 (adj price)
            (15, 38, 1, 45),// 24

            // Order 17: Jan, total=18 → Blaubeere×2@9 = 18
            (16, 42, 2, 9), // 25

            // Order 18: Lena, total=80 → Kirschbaum×2@40 = 80
            (17, 31, 2, 40),// 26

            // Order 19: Lena, total=14 → Weinrebe@14
            (18, 46, 1, 14),// 27

            // Order 20: Felix, total=32 → Feigenbaum@32
            (19, 34, 1, 32),// 28

            // Order 21: Felix, total=27 → Kohlrabi×9@3 = 27
            (20, 26, 9, 3), // 29

            // Order 22: Marie, total=55 → Japanischer Ahorn@55
            (21, 35, 1, 55),// 30

            // Order 23: Marie, total=66 → Passionsfrucht×3@22 = 66
            (22, 48, 3, 22),// 31

            // Order 24: Marie, total=40 → Granatapfel×2@20 = 40
            (23, 49, 2, 20),// 32

            // Order 25: Paul, total=96 → Rose rot×4@12 + Rose weiß×4@12 = 48+48=96
            (24, 11, 4, 12),// 33
            (24, 12, 4, 12),// 34

            // Order 26: Paul, total=25 → Birke@25
            (25, 36, 1, 25),// 35

            // Order 27: Noah, total=70 → Flieder×2@20 + Lavendel×5@6 = 40+30=70
            (26, 37, 2, 20),// 36
            (26, 2, 5, 6),  // 37

            // Order 28: Noah, total=44 → Johannisbeere×4@6 + Stachelbeere×2@6 + Brombeere×1@7 + Dill@1/adj
            // 24+12+7=43... Brombeere@7 + extra: use adj unit price
            // Johannisbeere×4@6=24 + Stachelbeere×2@6=12 + Brombeere×1@8(adj)=8 → 44
            (27, 43, 4, 6), // 38
            (27, 44, 2, 6), // 39
            (27, 45, 1, 8), // 40

            // Order 29: Noah, total=12 → Basilikum×4@3 = 12
            (28, 0, 4, 3),  // 41

            // Order 30: Emma, total=88 → Magnolie@48 + Wisteria@38 + Dill@2 = 88
            (29, 38, 1, 48),// 42
            (29, 39, 1, 38),// 43
            (29, 9, 1, 2),  // 44

            // Order 31: Emma, total=60 → Chrysantheme×10@6 = 60
            (30, 17, 10, 6),// 45

            // Order 32: Leon, total=75 → Hortensie×5@15 = 75
            (31, 18, 5, 15),// 46

            // Order 33: Leon, total=28 → Kiwi@18 + Dill×5@2 = 18+10=28
            (32, 47, 1, 18),// 47
            (32, 9, 5, 2),  // 48

            // Order 34: Elias, total=52 → Birke×2@25 + Dill@2 = 50+2=52
            (33, 36, 2, 25),// 49
            (33, 9, 1, 2),  // 50

            // Order 35: Elias, total=15 → Minze×5@3 = 15
            (34, 4, 5, 3),  // 51

            // Order 36: Ben, total=110 → Kirschbaum@40 + Apfelbaum@35 + Erdbeere×7@5 = 40+35+35=110
            (35, 31, 1, 40),// 52
            (35, 30, 1, 35),// 53
            (35, 40, 7, 5), // 54

            // Order 37: Lina, total=84 → Kiwi×2@18 + Magnolie@48 = 36+48=84
            (36, 47, 2, 18),// 55
            (36, 38, 1, 48),// 56

            // Order 38: Maximilian, total=30 → Mangold×5@4 + Spinat×3@3 + Dill@1/adj
            // 20+9=29 + Dill@1(adj) → nope. Mangold×5@4=20 + Spinat×2@3=6 + Dill×2@2=4 = 30
            (37, 29, 5, 4), // 57
            (37, 28, 2, 3), // 58
            (37, 9, 2, 2),  // 59

            // Order 39: Amelie, total=46 → Weinrebe×2@14 + Kiwi@18 = 28+18=46
            (38, 46, 2, 14),// 60
            (38, 47, 1, 18),// 61

            // Order 40: Anton, total=55 → Japanischer Ahorn@55
            (39, 35, 1, 55),// 62
        };

        // Fix order 3: remove wrong items, recalculate
        // Order 3 items (indices 6-9) should be: Apfelbaum@35 + Kirschbaum@40 + Erdbeere×9@5 = 120
        // Items array has indices 6=Apfelbaum@35, 7=Kirschbaum@40, 8=Erdbeere×3@5, 9=Flieder@20
        // That's 35+40+15+20=110, not 120. Fix: replace item 9 with Dill×5@2=10 → 35+40+15+10=100 still wrong.
        // Simplest: Apfelbaum@35 + Kirschbaum@40 + Erdbeere×9@5=45 → 120. So item 8 should be Erdbeere×9@5.
        // But we already wrote the array... let me just fix inline.

        for (var i = 0; i < items.Length; i++)
        {
            var it = items[i];

            // Fix order 3 (idx=2): item at array position 8 should be Erdbeere×9@5, remove item at position 9
            if (i == 8) it = (2, 40, 9, 5);  // Erdbeere×9@5 = 45 → total: 35+40+45=120
            if (i == 9) continue;              // skip the Flieder item, not needed

            var oid = orderIds[it.OrderIdx];
            var pid = plantIds[it.PlantIdx];
            db.Query($"upsert order_items {{ order_id: '{oid}', plant_id: '{pid}', quantity: {it.Qty}, unit_price: {it.UnitPrice} }}");
        }

        // ── Carts (TTL 5m) ── active shopping carts that expire after 5 minutes
        var carts = new (int CustIdx, int PlantIdx, int Qty, string Note, string? RowTtl)[]
        {
            (0, 2, 3, "für den Vorgarten", null),          // Anna: Lavendel×3, table TTL (5m)
            (1, 35, 1, "Geburtstagsgeschenk", null),       // Thomas: Jap. Ahorn, table TTL (5m)
            (2, 13, 10, "Frühlingsbestellung", null),       // Sophie: Tulpe gelb×10, table TTL (5m)
            (3, 33, 1, "", "10m"),                          // Markus: Olivenbaum, row TTL 10m override
            (4, 40, 5, "Balkon", null),                     // Laura: Erdbeere×5, table TTL (5m)
            (7, 20, 8, "Hochbeet", null),                   // Felix: Tomate×8, table TTL (5m)
            (15, 46, 2, "Geschenk Mama", "15m"),            // Noah: Weinrebe×2, row TTL 15m override
        };

        for (var i = 0; i < carts.Length; i++)
        {
            var c = carts[i];
            var cid = customerIds[c.CustIdx];
            var pid = plantIds[c.PlantIdx];
            var ttlPart = c.RowTtl is not null ? $", ttl: {c.RowTtl}" : "";
            db.Query($"upsert carts {{ customer_id: '{cid}', plant_id: '{pid}', quantity: {c.Qty}, note: '{c.Note}'{ttlPart} }}");
        }
    }
}

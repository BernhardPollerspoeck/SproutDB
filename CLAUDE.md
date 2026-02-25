# SproutDB - Claude Instructions

## Code Quality Rules

- **Keine null-forgiving operator (`!`)** - Immer saubere null-checks, guard clauses oder pattern matching statt `!`. Der `!` Operator versteckt Probleme statt sie zu lösen.
- Nullable reference types sind aktiviert (`<Nullable>enable</Nullable>`) - nutze sie korrekt.

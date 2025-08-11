# CREATE TABLE

## Description

The CREATE TABLE command creates a new empty table in the SproutDB database. Unlike traditional SQL databases, SproutDB follows a schema-less approach where tables are created without predefined columns, and schema evolves through usage or explicit column additions.

## Where It Applies

- Used as a standalone command outside of data queries
- Specifies only a new table name without column definitions
- Requires appropriate permissions (schema operation permission)
- Cannot create a table that already exists
- Creates an empty table ready to receive data

## What It Does

- Creates a new table in the current branch with the specified name
- Automatically creates only the required `id` column (SproutDB generated as base 128)
- Makes the table immediately available for data operations
- Creates a schema change record in the branch history
- Schema evolves either through:
  - Implicit evolution via upsert operations
  - Explicit column additions via `add column` command
- All fields except `id` are optional

## Examples

### Basic table creation

```sql
create table users
```

### Creating tables for a new feature

```sql
create table products
create table product_categories
create table product_tags
```

### Creating a table for temporary data

```sql
create table import_temp_data
```

### Creating a table in a feature branch

```sql
checkout branch feature/new-module
create table feature_settings
```

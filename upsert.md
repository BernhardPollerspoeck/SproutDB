# Upsert Documentation

## Overview
The `upsert` command is used to insert new records or update existing records based on a unique identifier. This command can be very useful in various scenarios where data needs to be maintained effectively.

## New Syntax for Bulk Updating All Records
### Command
```plaintext
upsert products all {price: price * 1.1}
```

### Meaning
This command updates the `price` field for all records in the `products` collection by multiplying the current price by `1.1`. Essentially, it applies a 10% increase to the price of each product.

### Usage Example
1. To increase the price of all products by 10%, you would use:
   ```plaintext
   upsert products all {price: price * 1.1}
   ```
2. To set a fixed price for all products, you could use:
   ```plaintext
   upsert products all {price: 100}
   ```

## Differences from Previous Bulk Upsert Syntax
Previously, bulk upsert syntax was limited to specific fields and required explicit values for each field. For example:
```plaintext
upsert products all {price: 100}
```
This syntax sets the `price` for all products to `100`, without any calculation or condition. The new syntax allows for more complex updates by using expressions, enabling dynamic updates based on current values.

## Conclusion
The new `upsert` syntax for bulk updating all records provides greater flexibility and efficiency in managing product prices, allowing developers to easily apply mathematical operations on existing fields.
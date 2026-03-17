# ME2M Data Cleaning

Use this cleaning pass before uploading any ME2M procurement file.

## Step 1: Keep Only Released Rows

Use the `Release indicator` column.

- Delete rows where `Release indicator` is blank.
- Delete rows where `Release indicator = B`.
- Keep only rows where `Release indicator = S`.

Rule:

```text
Keep row only if Release indicator = S
```

This prevents open, unreleased, or non-final procurement rows from entering the database.

## Step 2: Do Not Manually Rework Net Price

Do not manually drag formulas to normalize `Net Price`.

The app now derives procurement price during extraction as:

```text
Derived Net Price = Effective value / Order Quantity
```

Because of that:

- keep `Effective value` unchanged
- keep `Order Quantity` unchanged
- keep `Price Unit` from SAP for traceability
- do not manually overwrite `Net Price`

## Upload Expectation

The ME2M file should be uploaded only after:

- all non-`S` release-indicator rows are removed
- `Effective value` and `Order Quantity` are preserved exactly from SAP
- `Still to be delivered (qty)` is present and retained

## Quantity Logic Used In The App

The app now treats procurement quantity as:

```text
Procured Quantity = Order Quantity - Still to be delivered (qty)
```

So `Still to be delivered (qty)` must not be removed during cleaning.

# Employees database sample

This is a toy dataset that demonstrates merging values for two new columns back into a base dataset. It reproduces the walkthrough video here:

https://www.youtube.com/watch?v=Ru0qjqxJyww

## Files

* `bootstrap.csv` is the base dataset to import
* `add-start-date.csv` adds start dates for all rows to the database
* `add-end-date.csv` adds end dates for all rows to the database

## Workflow

1. Import `bootstrap.csv`
1. Add a new row for Matt
1. Create branches for each of `add-start-date.csv` and `add-end-date.csv`
1. On those branches, import the respective files to update the database
1. Merge those changes back to master

See the `script.sh` file for details
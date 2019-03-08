# State populations database sample

This is a toy dataset that demonstrates many revisons to a small number of rows.

## Files

* `1790.psv`, `1800.psv`, etc.: Yearly population data, keyed by state name.
* `1850_no_pop.psv`: separate dataset, with true / false values for statehood for many states as of that year
* `schema_with_is_a_state.json`: Schema definition for year datasets
* `schema_with_is_a_state.json`: Schema definition for `1850_no_pop.psv`
* `a-states.psv`: Subset of state data for states starting with 'a'
* `c-states.psv`: Subset of state data for states starting with 'c'
* `create_db.sh`: Bash script to create a dataset with all years' data.

## Workflow

See `create_db.sh` for details. Run the script to create a new dolt database with one commit for each year of data. The
script doesn't use the schema files because the schema can be inferred.

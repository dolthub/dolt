#!/usr/bin/env bats

setup() {
    REPO_NAME="dolt_repo_$$"
    mkdir $REPO_NAME
    cd $REPO_NAME

    dolt init
}

teardown() {
    cd ..
    rm -rf $REPO_NAME
}

@test "sakila: import sakila dump" {
    run dolt sql < ../sakila_dump.sql
    [ "$status" -eq 0 ]

    cd sakila

    # tables in working set
    run dolt ls
    [ "${#lines[@]}" -eq 17 ]

    # triggers
    run dolt sql -q "select trigger_name from information_schema.triggers;" -r csv
    [ "$output" = "trigger_name
customer_create_date
payment_date
rental_date
del_film
ins_film
upd_film" ]

    # views
    run dolt sql -q "select table_name from information_schema.views;" -r csv
    [ "$output" = "table_name
actor_info
customer_list
film_list
nicer_but_slower_film_list
sales_by_film_category
sales_by_store
staff_list" ]

    # procedures
    run dolt sql -q "select routine_name from information_schema.routines where routine_schema = 'sakila' and routine_type = 'PROCEDURE';" -r csv
    [[ "$output" =~ "film_in_stock" ]] || false

    run dolt sql -r csv <<SQL
CALL film_in_stock(1, 1, @aa);
SELECT @aa;
SQL
    [ "$status" -eq 0 ]
    [ "$output" = "inventory_id
1
2
3
4
@aa
4" ]

    # views are imported correctly
    run dolt sql -q "select count(*) from film_list"
    [[ "$output" =~ "997" ]] || false

    cd ..
}

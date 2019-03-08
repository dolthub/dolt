# To see each command being executed, invoke like so:
# bash -x script.sh
dolt init
dolt status
dolt branch
dolt table import employees --pk=id -c bootstrap.csv
dolt table select employees
dolt table select employees --where 'first name'=tim
dolt diff
dolt status
dolt add .
dolt status
dolt commit -m "initial employees table"
dolt status
dolt log
dolt table
dolt table put-row employees id:3 "first name":matt "last name":jesuele title:"software engineer"
dolt table select employees 
dolt diff
dolt status
dolt add employees
dolt status
dolt commit -m "added matt"
dolt status
dolt log
dolt diff
dolt branch add-start-date
dolt branch add-end-date
dolt branch
dolt checkout add-start-date
dolt branch --list
dolt table import -u employees add-start-date.csv
dolt diff
dolt status
dolt add employees
dolt commit -m "added start dates"
dolt log
dolt diff p8op1edihfk39qupcppcg6rhb9cgkdrr 11kajeka0p11am36ljlkrrrg9q888fmd
dolt branch
dolt checkout add-end-date
dolt table select employees
dolt table import -u employees add-end-date.csv
dolt add employees
dolt diff
dolt status
dolt commit -m "added end dates"
dolt log
dolt checkout master
dolt merge add-start-date
dolt log
dolt merge add-end-date
dolt status
dolt add .
dolt status
dolt commit -m 'merging end dates'
dolt log

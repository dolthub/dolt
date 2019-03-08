# Script to populate the dolt repo with data from 1790 to 1860 with one commit per year.
# Run with -x option to see commands echoed as they are executed.
dolt init
dolt table import -c --pk state state_pops 1790.psv; dolt add .; dolt commit -m '1790 data'
dolt table import -u state_pops 1800.psv; dolt add .; dolt commit -m '1800 data'
dolt table import -u state_pops 1810.psv; dolt add .; dolt commit -m '1810 data'
dolt table import -u state_pops 1820.psv; dolt add .; dolt commit -m '1820 data'
dolt table import -u state_pops 1830.psv; dolt add .; dolt commit -m '1830 data'
dolt table import -u state_pops 1840.psv; dolt add .; dolt commit -m '1840 data'
dolt table import -u state_pops 1850.psv; dolt add .; dolt commit -m '1850 data'
dolt table import -u state_pops 1860.psv; dolt add .; dolt commit -m '1860 data'
dolt log

The noms command is badly supported, but can be useful for certain debugging tasks. Notably, `noms manifest`
and `noms show` are kept in good working order. The rest are in varying states of brokenness, but may still be useful.

For inspecting the raw data of a dolt database, the `splunk.pl` script in this directory is your best bet. It
uses `noms manifest` and `noms show` in a simple shell to explore a tree of values and refs. 
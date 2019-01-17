# 1/17/19 Version 0.3.0
  * Merge
    *	dolt merge <branch>
    * dolt merge --abort
  * Display Conflicts
    * dolt conflicts cat [<commit>] <table>...
  * Resolve Conflicts
    * dolt conflicts resolve <table> <key>...
    *	dolt conflicts resolve --ours|--theirs <table>...

# 1/8/19 Version 0.2.2
  * Windows support.
  * Move data to .dolt/noms directory
    * To convert existing data repositories you will need to run:
      * mkdir .dolt/noms
      * mv * .dolt/noms/
      * rm * (If you have been using this directory to store other files you may need to be more deliberate about what you delete.)
  * Bug fixes.
  * Error messaging improvements.

# 1/6/19 Version 0.2.1
  * dolt table schema command
  * --all option for dolt add
  * table headers printed when using commands cat and diff
  * Bug fixes
  * Documentation Updates

# 1/4/19 Version 0.2.0
Initial internal release of dolt for team to be able to start playing with it.  Initial documentetation:


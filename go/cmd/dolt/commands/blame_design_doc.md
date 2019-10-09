# Dolt Blame

## Goal

Row-based blame annotation for Dolt tables (column-based coming soon)

Assume we have a `startup_employees` table with some data with schema `pk:int`,
`name:string`, and `salary:int`.

In the example below, the `commit`, `author`, and `time` columns are
added by `blame`.

```
# find out who last modified the software engineers
dolt blame HEAD startup_employees --where "salary >= 100000"

+----+---------+--------+----------------------------------+----------------+---------------------------+
| pk | name    | salary | commit                           | author         | time                      |
+----+---------+--------+----------------------------------+----------------+---------------------------+
| 1  | Alex    | 100000 | 91osuaj28kpbl8fc2jahq90nv0chpps5 | Barbara Dwyer  | 2019-04-07 14:37:48 -0700 |
| 2  | Barbara | 125000 | 91osuaj28kpbl8fc2jahq90nv0chpps5 | Barbara Dwyer  | 2019-04-07 14:37:48 -0700 |
| 5  | Robert  | 125000 | bluojp2qu05o7h2j055kmfskkqm0a28i | Robert Loblaw  | 2019-04-08 11:27:15 -0700 |
| 8  | Diana   | 120000 | vnolpfad1h277en46qq5084a0dfpkgju | Thomas Foolery | 2019-04-08 12:10:22 -0700 |
+----+---------+--------+----------------------------------+----------------+---------------------------+
```

## Approach

The approach that seems simplest is [what go-git does](https://github.com/src-d/go-git/blob/master/blame.go),
but I don't currently see any reason to do forwards traversal of history
like they do:

- Build a graph on the fly (i.e. when the command is run)
- For each row (uniquely identified across commits by primary key) in the query,
  add a node
- Each node starts with its blame pointing to the most recent commit
- Traverse backwards through history, and for each node that remains
  unfinalized, diff that row in the current commit against the previous commit:
  - If different, finalize the node
  - If same, set the node's blame to the previous commit
- When all nodes are marked finalized, display the blame

### Possible alternatives

1. Store blame information directly in cells or rows, presumably computed at
   commit-time
   - What about commits that already exist without this information?
2. Precompute the blame in a separate data structure like the one used in
   the chosen approach. This would also presumably happen at commit-time,
   but could also potentially happen as a background job

### Advantages to the chosen approach

- Fairly simple
- I pretty much know how to write it
- I know [where to get the sorted revision list from](https://github.com/liquidata-inc/dolt/blob/84d9eded517167eb2b1f76073df88e85665eec1d/go/libraries/doltcore/env/actions/commit.go#L111)
- Largely orthogonal (for example, shouldn't need to alter any existing
  models)

### Disadvantages to the chosen approach

- Guaranteed to be the slowest of the three at the time the command is run
- ???

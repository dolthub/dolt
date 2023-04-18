This document is to show the implementation theory behind the `MatchNode` structure in `expr_parser_node.go`.
The intent is to aid future development by giving a high-level overview of how the `dolt_branch_control` table works in relation to the `MatchNode`.

I'll first explain how it works by using an example.
The branch control tables operate over 4 columns: `database`, `branch`, `user`, and `host`.
For this example, I'll work with only the first two columns to make it a bit easier to work with.
Also, it's worth mentioning that the actual implementation works on sort orders[^1], however I'll use the original characters in this example, as well as the character stand-ins for the `singleMatch`[^2] and `anyMatch`[^3] characters.

## Insertion

The initial root `MatchNode` looks like the following:
```
Node("|", nil)
```
Our root node contains a single `columnMarker` (represented using `|`), which marks the beginning of a column.
In addition, it does not represent a destination node[^4], therefore it does not contain any data (represented with `nil` for no data, and `dataX` for data).
Let's say we want to add the database `"databomb"` and branch `"branchoo"`.
We concatenate the strings together, and insert `columnMarker`s before each column, which now gives us `"|databomb|branchoo"`.
Adding this new string to the `MatchNode` results in the following:
```
Node("|", nil)
 └─ Node("databomb|branchoo", data1)
```
You'll notice that the initial `columnMarker` was "consumed"[^5] by the root node, leaving the child with the same string sans the `columnMarker`.
Now, let's add the branch `"branchah"` with the same database:
```
Node("|", nil)
 └─ Node("databomb|branch", nil)
     ├─ Node("oo", data1)
     └─ Node("ah", data2)
```
The difference between `"|databomb|branchoo"` and `"|databomb|branchah"` begins after `"|branch"`, so we split the destination node into two children.
The first child contains the remainder of the original destination node (which is just `"oo"`) while the second child contains the remainder of the new branch (`"ah"`).
We've also moved our data (`data1`) to the new child, making the original node a parental[^6] node.
For our next addition, we'll add `"|databoost|branchius"`:
```
Node("|", nil)
 └─ Node("databo", nil)
     ├─ Node("mb|branch", nil)
     │   ├─ Node("oo", data1)
     │   └─ Node("ah", data2)
     └─ Node("ost|branchius", data3)
```
This splits the `"databomb|branch"` node, similar to when we added `"branchah"`.
`"databo"` is the common portion of `"databomb"` and `"databoost"`, so it becomes a parental node.
We still move the "data", but as it wasn't a destination node, we just moved `nil`.
Lastly, let's add `"|databomb|branchahee"`, which appends `"ee"` to our pre-existing branch `"branchah"`:
```
Node("|", nil)
 └─ Node("databo", nil)
     ├─ Node("mb|branch", nil)
     │   ├─ Node("oo", data1)
     │   └─ Node("ah", data2)
     │       └─ Node("ee", data4)
     └─ Node("ost|branchius", data3)
```
As `"branchah"` was a destination node, we simply add the `"ee"` portion as a child, while `"branchah"` remains a destination node.
As both are destination nodes, this means we can match both `"branchah"` and `"branchahee"`.

## Deletion

Let's use the tree from the last section:
```
Node("|", nil)
 └─ Node("databo", nil)
     ├─ Node("mb|branch", nil)
     │   ├─ Node("oo", data1)
     │   └─ Node("ah", data2)
     │       └─ Node("ee", data4)
     └─ Node("ost|branchius", data3)
```
First, we'll delete the first string we inserted (`"|databomb|branchoo"`):
```
Node("|", nil)
 └─ Node("databo", nil)
     ├─ Node("mb|branchah", data2)
     │   └─ Node("ee", data4)
     └─ Node("ost|branchius", data3)
```
Multiple things occurred here.
First, removing `"|databomb|branchoo"` only removed the `("oo", data1)` node, as the remainder of the string is still used by other strings.
This gave us the intermediate subtree:
```
 └─ Node("databo", nil)
     └─ Node("mb|branch", nil)
         └─ Node("ah", data2)
             └─ Node("ee", data4)
```
Since `Node("mb|branch", nil)` is _not_ a destination node, and it now contains only a single child, we can merge that child with the node.
This just appends the child's string to the node, moves its data, and moves its children.
It is worth noting that attempting to delete a node that is not a destination node results in a no-op[^7], as it must have multiple children, and its parent either has multiple children or is a destination node.
The only exception will be the root node which will actually merge its child onto itself.

## Matching

Using the tree from the last section, we'll illustrate how matching works.
For reference, this is our tree:
```
Node("|", nil)
 └─ Node("databo", nil)
     ├─ Node("mb|branchah", data2)
     │   └─ Node("ee", data4)
     └─ Node("ost|branchius", data3)
```
Let's attempt to match `"|databomb|branch"`.
First we'll match `|` on the root, leaving us with `"databomb|branch"`.
As children are implemented using a map, we do three checks: one for `%` (`anyMatch`), one for `_` (`singleMatch`), and another for the character itself.
These three checks are what allows us to use the `LIKE` expression syntax.
In our case, `%` and `_` do not return nodes, but `d` returns `Node("databo", nil)`, so we continue.
If we had multiple nodes then we would iterate over each node in turn, but as we only have one we fully match that node.
That leaves us with `"mb|branch"`.
Again we do three checks, and match `m` for `Node("mb|branchah", data2)`.
As we do not match the full node, we end up returning `false` for the match.

In order to match, you must fully match a destination node.
There is an exception, and that deals with nodes ending with an `anyMatch`.
To explain how these match, I'll skip the nodes and just use a single string, and I'll also use a table to track the match progress.
Let's say we have the expression `"abc%fg"` and we want to match the string `"abcdeffg"`.

| expressions | string   |
|-------------|----------|
| abc%fg      | abcdeffg |

`"abc"` will exactly match with the expression, so we'll remove it from both sides.

| expressions | string |
|-------------|--------|
| %fg         | deffg  |

`d` matches `%`, which consumes it on our string, but does _not_ consume it for our expression, leaving us with `"effg"` for our string.

| expressions | string |
|-------------|--------|
| %fg         | effg   |

Similarly, `e` is consumed on the string, leaving us with `"ffg"`.

| expressions | string |
|-------------|--------|
| %fg         | ffg    |

Now for the fun part, `f` still matches against `%`, but it also matches against the `f` after the `anyMatch`, so we create a new expression.
This means that we now match against two expressions (`string` is duplicated in the table, but we only ever compare against a single string).

| expressions | string |
|-------------|--------|
| %fg         | fg     |
| g           | fg     |

Whenever the character after an `anyMatch` would match the current character, we create a new expression.
Since we had two `f`s in our string, we end up matching against `%` again, along with the following `f` again, creating a third match.
However, the expression `g` does not match our second `f`, so it is dropped.
In this case though, the third that expression we just created is another `"g"`, so I'll cross out the previous `"g"` expression to make it clear that we added a new expression.

| expressions | string |
|-------------|--------|
| %fg         | g      |
| ~~g~~       | ~~g~~  |
| g           | g      |

Finally, our first and third expressions end up matching the final character in our string.
`"%fg"` matches because `anyMatch` still matches everything, and `"g"` is an exact match.
Now we've completely matched our string, and we've got two expressions (nodes in the actual implementation).
We throw out all expressions that still have unmatched characters, except if there is only one character in the expression **and** it is the `anyMatch` character.
`"%fg"` has more than one character and does not end with `anyMatch`, so it is thrown out, leaving us with only the third expression.

There are two additional points worth mentioning.
The first is that we fold all expressions using `FoldExpression()`, which reduces an expression down to its smallest form, and also guarantees the uniqueness of the expression.
This lets us catch all duplicate expressions.
The second is that `singleMatch` and `anyMatch` do not match `columnMarker`, and `singleMatch` does not match `anyMatch`.
This ensures that column boundaries are respected, and it also gives us the ability to match expressions against other expressions to find subsets.
For example, `"abc%"` will match all strings that `"abc__"` would match, meaning `"abc__"` will only match a subset of `"abc%"`, which allows us to also block the insertion of subsets.
We cannot efficiently find supersets to remove existing nodes, so subsets may still appear in the tree depending on the order of insertion.

[^1]: A sort order is the comparing value of a character, digit, or symbol. This allows for things such as case-insensitive comparisons, as an uppercase and lowercase character will have the same sort order. They're ignored in this document as they're reliant on collations, and they'd be hard to visually parse.

[^2]: The `singleMatch` character matches any single character, digit, or symbol, except for the `anyMatch` and `columnMarker` characters. 

[^3]: The `anyMatch` character will match zero or more characters, digits, or symbols. It does not match `columnMarker`s.

[^4]: Destination nodes are nodes that contain data, and represent a full expression when all parents are concatenated together along with this node's expression. Matches may only be done on destination nodes, and destination nodes may still have children. We do not have a concept of a "leaf node", hence the term "destination node".

[^5]: The code assumes that all terminal nodes are either destination or parental nodes. This is true for all nodes under the root, but will not be true for the initial root node. To make the code simpler, we don't special case the root node since it's not that important or impactful to performance. The root node can still end up as a destination node by adding two children and then deleting one of them.

[^6]: Parental nodes are nodes that contain only children (with no data), and must contain at least two children (except the root node).

[^7]: This includes the root node, as it will be the only node without a parent, and removing a node from the tree requires removing it from its parent.
// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/util/outputpager"
	"github.com/fatih/color"
)

/**
* The graph API is used to draw a text-based representation of the commit
* history. The API generates the graph in a line-by-line fashion.
*
* The Algorithm
* ----------------
*
* - Calculate the positions of the commits in the graph.
*   The vertical position of each commit is determined by the order of the commits, but should be adjusted to the length of the commit message.
*
*   The calculation of horizontal position is more complex, but mostly depends on the parent-child relationship of the commits. This is done in the function computeColumnEnds.
*
*   Create a 2D matrix to store the branch paths, this matrix will help us find the available column for the commits.
*   In each column, there will be multiple branch paths, and each path is represented by a pair of positions of start and end commits on the branch.
*   For example:
*    0 * Head commit (main branch)
*    1 *  Merge branch A into main
*    2  *  Commit on branch A
*    3 *  Parent commit 1
*    4 *  Merge branch B into main
*	 5  *  Commit on branch B
*	 6 *  Parent commit 2
*   Branches in this will be stored in columns as:
*   Column 0: [[0,6]], there's only one branch path in this column, from the head commit to the parent commit 2
*   Column 1: [[2,2], [5,5]], there are two branch paths in this column
*
*   Note that we are calculating the positions in the sorted order, so the child commits are always calculated before their parents.
*	 There are 3 types of commits:
*   1. The commit has no children, this is a head commit on a branch. It will be put in a new column
*	2. The commit has branch children (at least one child has this commit as first parent), this commit will put in the column of the left most branch children.
*   3. The commit has no branch children but merge children, searching in the columns matrix to find the first column that has no branches that will overlap with the current commit.
*		- In the last example, when determining where to put the commit on branch B, we should have the columns matrix as: [[[0,6]], [[2,2]]]. Potential column index options for this commit is 0, 1, or we can create a new column: 2.
*		- Look at the children of this current commit, which is the merge commit and it's on column 0. So we can start from column 1,and since the path in column 1 ends at row 2, this commits can be put in column 1.
*
*   After both the vertical and horizontal positions are calculated, we will expand the graph based on the length of the commit message, this part is done in the function expandGraph.
*
* - Draw the graph. The graph is drawn in function drawCommitDotsAndBranchPaths.
*   Once we have the positions of the commits, we can draw the graph.
*   For each commit, draw the commit and the path to its parent(s).
*   Same as the calculation part, we have 3 types of paths:
*   1. The parent is on the same branch/column, draw a vertical line from the parent to the current commit.
*   2. The parent is on the left side of the current commit. Draw a diagonal line from the parent to the column of the current commit, then draw a horizontal line or a vertical line depending on the vertical and horizontal distance of the two.
*          a. the horizontal distance is greater than the vertical distance                               b. the vertical distance is greater than the horizontal distance
*                                    * child commit                                                                                      * child commit
*                                   /                                                                                                    |
*                                  /                                                                                                    /
*                            *----  parent commit                                                                                      * parent commit
*
*   3. The parent is on the right side of the current commit, draw a diagonal line from the parent to the column of the current commit, then draw a horizontal line to the parent.
*          a. the vertical distance is greater than the horizontal distance	                              b. the horizontal distance is greater than the vertical distance
*                           * child commit                                                                                          * --- child commit
*                            \                                                                                                            \
*                             \                                                                                                            \
*                             |                                                                                                             \
*                             * parent commit                                                                                                * parent commit
*
* ------------
* Sample output
* -------------
*
* The following is an example of the output.
* ------------

*   commit skcm452jteobhaqb2pngc8rbc37p0p0d(HEAD -> main)
|\  Merge: qcfirrj62mlvv6002bq6m9ep16t4ej61 tnenj0ntvhue15v3veq98ng4pi2vvqta
| | Author: liuliu <liu@dolthub.com>
| | Date: Thu Aug 01 10:23:22 -0700 2024
| |
| |     Merge branch 'd' into main
| |
* |   commit qcfirrj62mlvv6002bq6m9ep16t4ej61
|\|   Merge: nlmgt75rl0a4j6ff8qp8afj2t70l3dfo 78vpv0ij6mr788vnm3k8r36m4k519ob1
| \   Author: liuliu <liu@dolthub.com>
| |\  Date: Thu Aug 01 10:23:19 -0700 2024
| | |
| | |   Merge branch 'c' into main
| | |
* | |   commit nlmgt75rl0a4j6ff8qp8afj2t70l3dfo
|\| |   Merge: l49as4tsfnph2v5edopu8a5cljs8vbc6 oj2frj1292visbj1ve14amhpcq0hn959
| \ |   Author: liuliu <liu@dolthub.com>
| |\|   Date: Thu Aug 01 10:23:16 -0700 2024
| | \
| | |\          Merge branch 'b' into main
| | | |
* | | | commit l49as4tsfnph2v5edopu8a5cljs8vbc6
|\| | | Merge: dk7dd4v2fhj40q3bbv0no9s1vpq41qef 7723gb3u9b125gum67dgier1t67s5md0
| \ | | Author: liuliu <liu@dolthub.com>
| |\| | Date: Thu Aug 01 10:22:59 -0700 2024
| | \ |
| | |\|         Merge branch 'a' into main
| | | \
* | | |\  commit dk7dd4v2fhj40q3bbv0no9s1vpq41qef
| | | | | Author: liuliu <liu@dolthub.com>
| | | | | Date: Thu Aug 01 10:22:54 -0700 2024
| | | | |
| | | | |       change
| | | | |
| * | | | commit tnenj0ntvhue15v3veq98ng4pi2vvqta(d)
| | | | | Author: liuliu <liu@dolthub.com>
| | | | | Date: Thu Aug 01 10:22:43 -0700 2024
| | | | |
| | | | |       d
| | | | |
| | * | | commit 78vpv0ij6mr788vnm3k8r36m4k519ob1(c)
| | | | | Author: liuliu <liu@dolthub.com>
| | | | | Date: Thu Aug 01 10:22:30 -0700 2024
| | | | |
| | | | |       c
| | | | |
| | | * | commit oj2frj1292visbj1ve14amhpcq0hn959(b)
| | | | | Author: liuliu <liu@dolthub.com>
| | | | | Date: Thu Aug 01 10:22:14 -0700 2024
| | | | |
| | | | |       b
| | | | |
| | | | * commit 7723gb3u9b125gum67dgier1t67s5md0(a)
| | |/ /  Author: liuliu <liu@dolthub.com>
| | / /   Date: Thu Aug 01 10:21:57 -0700 2024
| |/ /
| / /           a
|/ /
*-- commit lqoem7sk4l1qjbdlf8k973o89bki0vlk
|   Author: liuliu <liu@dolthub.com>
|   Date: Thu Aug 01 10:21:37 -0700 2024
|
|       main
|
* commit 87bg8tfrvjo8cfbak92flu595p5e4bbl
  Author: liuliu <liu@dolthub.com>
  Date: Thu Aug 01 10:20:44 -0700 2024

       Initialize data repository

* ------------
*
*/

type commitInfoWithChildren struct {
	Commit           CommitInfo
	Children         []string
	Col              int
	Row              int
	formattedMessage []string
}

var branchColors = []*color.Color{
	color.New(color.FgRed),
	color.New(color.FgGreen),
	color.New(color.FgBlue),
	color.New(color.FgMagenta),
	color.New(color.FgCyan),
	color.New(color.FgWhite),
}

type branchPath struct {
	Start int
	End   int
}

// mapCommitsWithChildrenAndPosition gets the children of commits, and initialize the x and y coordinates of the commits
func mapCommitsWithChildrenAndPosition(commits []CommitInfo) []*commitInfoWithChildren {
	childrenMap := make(map[string][]string)
	for _, commit := range commits {
		for _, parent := range commit.parentHashes {
			childrenMap[parent] = append(childrenMap[parent], commit.commitHash)
		}
	}

	var commitsWithChildren []*commitInfoWithChildren
	for index, commit := range commits {
		commitsWithChildren = append(commitsWithChildren, &commitInfoWithChildren{
			Commit:   commit,
			Children: childrenMap[commit.commitHash],
			Col:      -1,
			Row:      index,
		})
	}

	return commitsWithChildren
}

func minVal(values ...int) int {
	if len(values) == 0 {
		return math.MaxInt
	}
	minVal := values[0]
	for _, v := range values {
		if v < minVal {
			minVal = v
		}
	}
	return minVal
}

// computeColumnEnds compute the column coordinate of each commit
func computeColumnEnds(commits []*commitInfoWithChildren, commitsMap map[string]*commitInfoWithChildren) ([]*commitInfoWithChildren, map[string]*commitInfoWithChildren) {
	columns := [][]branchPath{}
	colPositions := make(map[string]int)
	newCommitMap := make(map[string]*commitInfoWithChildren)
	commitsWithColPos := make([]*commitInfoWithChildren, len(commits))

	updateColumns := func(col, end int) {
		columns[col][len(columns[col])-1].End = end
	}

	for index, commit := range commits {
		var branchChildren []string
		for _, child := range commit.Children {
			if commitsMap[child].Commit.parentHashes[0] == commit.Commit.commitHash {
				branchChildren = append(branchChildren, child)
			}
		}

		isLastCommitOnBranch := len(commit.Children) == 0
		isBranchOutCommit := len(branchChildren) > 0

		commitColInd := -1

		if isLastCommitOnBranch {
			columns = append(columns, []branchPath{
				{
					Start: index,
					End:   index,
				},
			})
			commitColInd = len(columns) - 1
		} else if isBranchOutCommit {
			// in the case of a branch out commit, the column index of the commit is the minimum column index of its children
			var branchChildrenColIndexes []int
			for _, childHash := range branchChildren {
				if childColInd, ok := colPositions[childHash]; ok {
					branchChildrenColIndexes = append(branchChildrenColIndexes, childColInd)
				}
			}

			commitColInd = minVal(branchChildrenColIndexes...)

			updateColumns(commitColInd, index)

			// update the path that branches out from the current commit by setting their end to be one position before the current commit
			for _, childColInd := range branchChildrenColIndexes {
				if childColInd != commitColInd {
					updateColumns(childColInd, index-1)
				}
			}
		} else {
			// minChildRowInd is the highest pos of child commit, maxChildColInd is the right most pos of child commit
			minChildRowInd := math.MaxInt
			maxChildColInd := -1
			for _, child := range commit.Children {
				if commitsMap[child].Row < minChildRowInd {
					minChildRowInd = commitsMap[child].Row
				}
				if colPositions[child] > maxChildColInd {
					maxChildColInd = colPositions[child]
				}
			}

			// find the first column that has no branches that overlap with the current commit
			// if no such column is found, put the commit in a new column
			col := -1
			for i := maxChildColInd + 1; i < len(columns); i++ {
				if minChildRowInd >= columns[i][len(columns[i])-1].End {
					col = i
					break
				}
			}
			if col == -1 {
				columns = append(columns, []branchPath{
					{
						Start: minChildRowInd + 1,
						End:   index,
					},
				})
				commitColInd = len(columns) - 1
			} else {
				commitColInd = col
				columns[col] = append(columns[col], branchPath{
					Start: minChildRowInd + 1,
					End:   index,
				})
			}
		}
		colPositions[commit.Commit.commitHash] = commitColInd
		commitsWithColPos[index] = &commitInfoWithChildren{
			Commit:   commit.Commit,
			Children: commit.Children,
			Col:      commitColInd,
			Row:      commit.Row,
		}
		newCommitMap[commit.Commit.commitHash] = commitsWithColPos[index]
	}
	return commitsWithColPos, newCommitMap
}

func printLine(graph [][]string, col, row int, pager *outputpager.Pager, line string, commit CommitInfo, decoration string) {
	graphLine := strings.Join(graph[row], "")

	emptySpace := strings.Repeat(" ", col-len(graph[row]))
	pager.Writer.Write([]byte(fmt.Sprintf("%s%s %s", graphLine, emptySpace, line)))

	if decoration != "no" {
		printRefs(pager, &commit, decoration)
	}
	pager.Writer.Write([]byte("\n"))
}

func printCommitMetadata(graph [][]string, pager *outputpager.Pager, row, col int, commit *commitInfoWithChildren, decoration string) {
	printLine(graph, col, row, pager, color.YellowString("commit %s", commit.Commit.commitHash), commit.Commit, decoration)

	printMergeInfo := 0
	if len(commit.Commit.parentHashes) > 1 {
		printMergeInfo = 1
	}
	if printMergeInfo == 1 {
		printLine(graph, col, row+1, pager, color.WhiteString("Merge: %s", strings.Join(commit.Commit.parentHashes, " ")), commit.Commit, "no")
	}

	printLine(graph, col, row+1+printMergeInfo, pager, color.WhiteString("Author: %s <%s>", commit.Commit.commitMeta.Name, commit.Commit.commitMeta.Email), commit.Commit, "no")

	printLine(graph, col, row+2+printMergeInfo, pager, color.WhiteString("Date: %s", commit.Commit.commitMeta.FormatTS()), commit.Commit, "no")

	pager.Writer.Write([]byte(strings.Join(graph[row+3+printMergeInfo], "")))
	pager.Writer.Write([]byte("\n"))

}

func trimTrailing(row []string) []string {
	lastIndex := len(row) - 1

	for lastIndex >= 0 && strings.TrimSpace(row[lastIndex]) == "" {
		lastIndex--
	}

	return row[:lastIndex+1]
}

// the height that a commit will take up in the graph
// 4 lines for commit metadata (commit hash, author, date, and an empty line) + number of lines in the commit message
// if the commit is a merge commit, add one more line for the "Merge:" line
func getHeightOfCommit(commit *commitInfoWithChildren) int {
	height := 4 + len(commit.formattedMessage)
	if len(commit.Commit.parentHashes) > 1 {
		height = height + 1
	}
	return height
}

func printOneLineGraph(graph [][]string, pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []*commitInfoWithChildren) {
	decoration := apr.GetValueOrDefault(cli.DecorateFlag, "auto")
	// print the first commit
	pager.Writer.Write([]byte(fmt.Sprintf("%s %s ", strings.Join(graph[commits[0].Row], ""), color.YellowString("commit %s", commits[0].Commit.commitHash))))
	if decoration != "no" {
		printRefs(pager, &commits[0].Commit, decoration)
	}
	pager.Writer.Write([]byte(color.WhiteString("%s\n", strings.Join(commits[0].formattedMessage, " "))))

	previousRow := commits[0].Row
	for i := 1; i < len(commits); i++ {
		// print the graph lines between the previous commit and the current commit
		for j := previousRow + 1; j < commits[i].Row; j++ {
			pager.Writer.Write([]byte(strings.Join(graph[j], "")))
			pager.Writer.Write([]byte("\n"))
		}

		pager.Writer.Write([]byte(fmt.Sprintf("%s %s ", strings.Join(graph[commits[i].Row], ""), color.YellowString("commit%s ", commits[i].Commit.commitHash))))
		if decoration != "no" {
			printRefs(pager, &commits[i].Commit, decoration)
		}
		pager.Writer.Write([]byte(color.WhiteString("%s\n", strings.Join(commits[i].formattedMessage, " "))))
		previousRow = commits[i].Row
	}
}

// printGraphAndCommitsInfo prints the commit messages in the graph matrix
func printGraphAndCommitsInfo(graph [][]string, pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []*commitInfoWithChildren) {
	decoration := apr.GetValueOrDefault(cli.DecorateFlag, "auto")

	for i := 0; i < len(commits)-1; i++ {
		startRow := commits[i].Row
		endRow := commits[i+1].Row
		startCol := commits[i].Col + 1

		// find the maximum width of the graph in the range startRow to endRow
		// this is used to align the commit message with the graph without overlapping with the graph
		for j := startRow; j < endRow; j++ {
			if len(graph[j]) > startCol {
				startCol = len(graph[j])
			}
		}

		printCommitMetadata(graph, pager, startRow, startCol, commits[i], decoration)

		commitInfoHeight := getHeightOfCommit(commits[i])

		for j, line := range commits[i].formattedMessage {
			row := startRow + commitInfoHeight - len(commits[i].formattedMessage) + j
			printLine(graph, startCol, row, pager, color.WhiteString("\t%s", line), commits[i].Commit, "no")
		}

		// print the remaining lines of the graph of the current commit
		for j := startRow + commitInfoHeight; j < endRow; j++ {
			pager.Writer.Write([]byte(strings.Join(graph[j], "")))
			pager.Writer.Write([]byte("\n"))
		}
	}

	last_commit_row := commits[len(commits)-1].Row
	printCommitMetadata(graph, pager, last_commit_row, len(graph[last_commit_row]), commits[len(commits)-1], decoration)
	for _, line := range commits[len(commits)-1].formattedMessage {
		pager.Writer.Write([]byte(color.WhiteString("\t", line)))
		pager.Writer.Write([]byte("\n"))
	}
}

// expandGraphBasedOnGraphShape expands the graph based on the shape of the graph, to give diagonal lines enough space to be drawn
func expandGraphBasedOnGraphShape(commits []*commitInfoWithChildren, commitsMap map[string]*commitInfoWithChildren) {
	posY := 0
	for i, commit := range commits {
		commit.Col = commit.Col * 2
		commit.formattedMessage = []string{strings.Replace(commit.Commit.commitMeta.Description, "\n", " ", -1)}
		if i > 0 {
			posY += 1
			for _, childHash := range commit.Children {
				if child, ok := commitsMap[childHash]; ok {
					horizontalDistance := math.Abs(float64(commit.Col - child.Col))
					if horizontalDistance+float64(child.Row) > float64(posY) {
						posY = int(horizontalDistance + float64(child.Row))
					}
				}
			}
		}
		commit.Row = posY
	}
}

// expandGraphBasedOnCommitMetaDataHeight expands the graph based on the length of the commit metadata
// the height of the commit is determined by the length of the commit message, if the commit is a merge commit, author, and date
func expandGraphBasedOnCommitMetaDataHeight(commits []*commitInfoWithChildren) {
	posY := 0
	for _, commit := range commits {
		// one empty column between each branch path
		commit.Col = commit.Col * 2
		commit.Row = posY
		formattedMessage := strings.Split(commit.Commit.commitMeta.Description, "\n")
		commit.formattedMessage = formattedMessage

		posY += getHeightOfCommit(commit) + 1
	}
}

func drawCommitDotsAndBranchPaths(commits []*commitInfoWithChildren, commitsMap map[string]*commitInfoWithChildren) [][]string {
	maxWidth, maxHeigh := 0, 0
	for _, commit := range commits {
		if commit.Col > maxWidth {
			maxWidth = commit.Col
		}
		if commit.Row > maxHeigh {
			maxHeigh = commit.Row
		}
	}
	heightOfLastCommit := getHeightOfCommit(commits[len(commits)-1])
	graph := make([][]string, maxHeigh+heightOfLastCommit)
	for i := range graph {
		graph[i] = make([]string, maxWidth+2)
		for j := range graph[i] {
			graph[i][j] = " "
		}
	}

	for _, commit := range commits {
		col := commit.Col
		row := commit.Row
		graph[row][col] = color.WhiteString("*")

		for _, parentHash := range commit.Commit.parentHashes {
			if parent, ok := commitsMap[parentHash]; ok {
				parentCol := parent.Col
				parentRow := parent.Row
				if parentCol == col {
					branchColor := branchColors[col/2%len(branchColors)]
					for r := row + 1; r < parentRow; r++ {
						if graph[r][col] == " " {
							graph[r][col] = branchColor.Sprintf("|")
						}
					}
				}
				if parentCol < col {
					branchColor := branchColors[col/2%len(branchColors)]
					horizontalDistance := col - parentCol
					verticalDistance := parentRow - row
					if horizontalDistance > verticalDistance {
						for i := 1; i < verticalDistance; i++ {
							graph[parentRow-i][parentCol+horizontalDistance-verticalDistance+i] = branchColor.Sprintf("/")
						}
						for i := parentCol; i < parentCol+(horizontalDistance-verticalDistance)+1; i++ {
							if graph[parentRow][i] == " " {
								graph[parentRow][i] = branchColor.Sprintf("-")
							}
						}
					} else {
						for i := parentCol + 1; i < col; i++ {
							graph[parentRow+parentCol-i][i] = branchColor.Sprintf("/")
						}
						for i := parentRow + parentCol - col; i > row; i-- {
							if graph[i][col] == " " {
								graph[i][col] = branchColor.Sprintf("|")
							}
						}
					}
				}
				if parentCol > col {
					branchColor := branchColors[parentCol/2%len(branchColors)]
					horizontalDistance := parentCol - col
					verticalDistance := parentRow - row
					if verticalDistance > horizontalDistance {
						for i := col + 1; i < parentCol; i++ {
							graph[row+i-col][i] = branchColor.Sprintf("\\")
						}
						for i := row + parentCol - col; i < parentRow; i++ {
							if graph[i][parent.Col] == " " {
								graph[i][parent.Col] = branchColor.Sprintf("|")
							}
						}
					} else {
						for i := 0; i < verticalDistance; i++ {
							graph[parentRow-i][parentCol-i] = branchColor.Sprintf("\\")
						}
						for i := col + 1; i < parent.Col-verticalDistance+1; i++ {
							if graph[row][i] == " " {
								graph[row][i] = branchColor.Sprintf("s-")
							}
						}
					}
				}
			}
		}
	}
	return graph
}

func logGraph(pager *outputpager.Pager, apr *argparser.ArgParseResults, commitInfos []CommitInfo) {
	color.NoColor = false

	commits := mapCommitsWithChildrenAndPosition(commitInfos)
	commitsMap := make(map[string]*commitInfoWithChildren)
	for _, commit := range commits {
		commitsMap[commit.Commit.commitHash] = commit
	}

	commits, commitsMap = computeColumnEnds(commits, commitsMap)
	oneLine := apr.Contains(cli.OneLineFlag)
	if oneLine {
		expandGraphBasedOnGraphShape(commits, commitsMap)
	} else {
		expandGraphBasedOnCommitMetaDataHeight(commits)
	}

	graph := drawCommitDotsAndBranchPaths(commits, commitsMap)

	// trim the trailing empty space of each line so we can use the length of the line to align the commit message
	for i, line := range graph {
		line = trimTrailing(line)
		graph[i] = line
	}
	if oneLine {
		printOneLineGraph(graph, pager, apr, commits)
	} else {
		printGraphAndCommitsInfo(graph, pager, apr, commits)
	}

}

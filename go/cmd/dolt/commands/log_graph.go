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
)

type CommitInfoWithChildren struct {
	Commit           CommitInfo
	Children         []string
	X                int
	Y                int
	formattedMessage []string
}

var branchColors = []string{
	"\033[31m", // Red
	"\033[32m", // Green
	"\033[34m", // Blue
	"\033[35m", // Magenta
	"\033[36m", // Cyan
	"\033[39m", // Default
}

type BranchPathType struct {
	Start int
	End   int
}

// get the children of commits, and initialize the x and y coordinates of the commits
func mapCommitsWithChildrenAndPosition(commits []CommitInfo) []*CommitInfoWithChildren {
	childrenMap := make(map[string][]string)
	for _, commit := range commits {
		for _, parent := range commit.parentHashes {
			childrenMap[parent] = append(childrenMap[parent], commit.commitHash)
		}
	}

	var commitsWithChildren []*CommitInfoWithChildren
	for y, commit := range commits {
		commitsWithChildren = append(commitsWithChildren, &CommitInfoWithChildren{
			Commit:   commit,
			Children: childrenMap[commit.commitHash],
			X:        -1,
			// the y coordinate of the commit is initialized to the index of the commit as the commits are sorted
			Y: y,
		})
	}

	return commitsWithChildren
}

// wrap the commit message in a constrained width to better align the commit message with the graph
func wrapTextOnWidth(text string, width int) []string {
	lines := strings.Split(text, "\n")
	totalRows := 0
	wrappedLines := make([]string, 0)

	for _, line := range lines {
		words := strings.Fields(line)
		currentLine := ""
		for _, word := range words {
			if len(currentLine)+len(word)+1 > width {
				wrappedLines = append(wrappedLines, currentLine)
				totalRows++
				currentLine = word
			} else {
				if currentLine != "" {
					currentLine += " "
				}
				currentLine += word
			}
		}
		if currentLine != "" {
			wrappedLines = append(wrappedLines, currentLine)
			totalRows++
		}
	}
	return wrappedLines
}

func min(values ...int) int {
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

// compute the X coordinate of each commit
func computeColumnEnds(commits []*CommitInfoWithChildren, commitsMap map[string]*CommitInfoWithChildren) ([]*CommitInfoWithChildren, map[string]*CommitInfoWithChildren) {
	// each column might have multiple branch paths, and the columns slice stores the branch paths of each column
	columns := [][]BranchPathType{}
	xPositions := make(map[string]int)
	newCommitMap := make(map[string]*CommitInfoWithChildren)
	commitsWithXPos := make([]*CommitInfoWithChildren, len(commits))

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

		commitX := -1

		if isLastCommitOnBranch {
			columns = append(columns, []BranchPathType{
				{
					Start: index,
					End:   index,
				},
			})
			commitX = len(columns) - 1
		} else if isBranchOutCommit {
			// in the case of a branch out commit, the x coordinate of the commit is the minimum x coordinate of its children
			var branchChildrenXs []int
			for _, childHash := range branchChildren {
				if childX, ok := xPositions[childHash]; ok {
					branchChildrenXs = append(branchChildrenXs, childX)
				}
			}

			commitX = min(branchChildrenXs...)

			updateColumns(commitX, index)

			// update the path that branches out from the current commit by setting their end to be one position before the current commit
			for _, childX := range branchChildrenXs {
				if childX != commitX {
					updateColumns(childX, index-1)
				}
			}
		} else {
			// Find an available column so the commit can connect to its children (merge commit) without overlapping with existing branches on columns
			// Otherwise, put the commit in a new column

			// minChildY is the highest pos of child commit, maxChildX is the right most pos of child commit
			minChildY := math.MaxInt
			maxChildX := -1
			for _, child := range commit.Children {
				if commitsMap[child].Y < minChildY {
					minChildY = commitsMap[child].Y
				}
				if xPositions[child] > maxChildX {
					maxChildX = xPositions[child]
				}
			}

			// find the first column that has no branches that overlap with the current commit
			col := -1
			for i := maxChildX + 1; i < len(columns); i++ {
				if minChildY >= columns[i][len(columns[i])-1].End {
					col = i
					break
				}
			}

			// if no column is found, put the commit in a new column
			if col == -1 {
				columns = append(columns, []BranchPathType{
					{
						Start: minChildY + 1,
						End:   index,
					},
				})
				commitX = len(columns) - 1
			} else {
				commitX = col
				columns[col] = append(columns[col], BranchPathType{
					Start: minChildY + 1,
					End:   index,
				})
			}
		}
		xPositions[commit.Commit.commitHash] = commitX
		commitsWithXPos[index] = &CommitInfoWithChildren{
			Commit:   commit.Commit,
			Children: commit.Children,
			X:        commitX,
			Y:        commit.Y,
		}
		newCommitMap[commit.Commit.commitHash] = commitsWithXPos[index]
	}
	return commitsWithXPos, newCommitMap
}

func printLine(graph [][]string, posX, posY int, pager *outputpager.Pager, line string, commit CommitInfo, color, decoration string) {
	graphLine := strings.Join(graph[posY], "")

	emptySpace := strings.Repeat(" ", posX-len(graph[posY]))
	pager.Writer.Write([]byte(fmt.Sprintf("%s%s%s %s", graphLine, emptySpace, color, line)))

	if decoration != "no" {
		printRefs(pager, &commit, decoration)
	}
	pager.Writer.Write([]byte("\n"))
}

func printCommitMetadata(graph [][]string, pager *outputpager.Pager, posY, posX int, commit *CommitInfoWithChildren, decoration string) {
	printLine(graph, posX, posY, pager, fmt.Sprintf("commit %s", commit.Commit.commitHash), commit.Commit, "\033[33m", decoration)

	printMergeInfo := 0
	if len(commit.Commit.parentHashes) > 1 {
		printMergeInfo = 1
	}
	if printMergeInfo == 1 {
		printLine(graph, posX, posY+1, pager, fmt.Sprintf("Merge: %s", strings.Join(commit.Commit.parentHashes, " ")), commit.Commit, "\033[37m", "no")
	}

	printLine(graph, posX, posY+1+printMergeInfo, pager, fmt.Sprintf("Author: %s <%s>", commit.Commit.commitMeta.Name, commit.Commit.commitMeta.Email), commit.Commit, "\033[37m", "no")

	printLine(graph, posX, posY+2+printMergeInfo, pager, fmt.Sprintf("Date: %s", commit.Commit.commitMeta.FormatTS()), commit.Commit, "\033[37m", "no")

	pager.Writer.Write([]byte(strings.Join(graph[posY+3+printMergeInfo], "")))
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
func getHeightOfCommit(commit *CommitInfoWithChildren) int {
	height := 4 + len(commit.formattedMessage)
	if len(commit.Commit.parentHashes) > 1 {
		height = height + 1
	}
	return height
}

// print the commit messages in the graph matrix
func appendMessage(graph [][]string, pager *outputpager.Pager, apr *argparser.ArgParseResults, commits []*CommitInfoWithChildren) {
	decoration := apr.GetValueOrDefault(cli.DecorateFlag, "auto")

	for i := 0; i < len(commits)-1; i++ {
		startY := commits[i].Y
		endY := commits[i+1].Y
		startX := commits[i].X + 1

		// find the maximum x position of the graph in the range startY to endY
		// this is used to align the commit message with the graph without overlapping with the graph
		for j := startY; j < endY; j++ {
			if len(graph[j]) > startX {
				startX = len(graph[j])
			}
		}

		printCommitMetadata(graph, pager, startY, startX, commits[i], decoration)

		commitInfoHeight := getHeightOfCommit(commits[i])

		// print the graph with commit message
		for j, line := range commits[i].formattedMessage {
			y := startY + commitInfoHeight - len(commits[i].formattedMessage) + j
			printLine(graph, startX, y, pager, fmt.Sprintf("\t%s", line), commits[i].Commit, "\033[37m", "no")
		}

		// print the remaining lines of the graph of the current commit
		for j := startY + commitInfoHeight; j < endY; j++ {
			pager.Writer.Write([]byte(strings.Join(graph[j], "")))
			pager.Writer.Write([]byte("\n"))
		}
	}

	last_commit_y := commits[len(commits)-1].Y
	printCommitMetadata(graph, pager, last_commit_y, len(graph[last_commit_y]), commits[len(commits)-1], decoration)
	for _, line := range commits[len(commits)-1].formattedMessage {
		pager.Writer.Write([]byte(fmt.Sprintf("\t\033[37m%s", line)))
		pager.Writer.Write([]byte("\n"))
	}
}

// expand the graph based on the length of the commit message
func expandGraph(commits []*CommitInfoWithChildren, width int) {
	posY := 0
	for i, commit := range commits {
		// one empty column between each branch path
		commit.X = commit.X * 2
		commit.Y = posY
		formattedMessage := wrapTextOnWidth(commit.Commit.commitMeta.Description, width)
		commit.formattedMessage = formattedMessage

		// make sure there is enough space for the diagonal line connecting to the parent
		// this is an approximation, assume that there will be enough space if parent is not the next commit
		maxDistanceFromParent := float64(0)
		if i < len(commits)-1 && commits[i+1].Commit.commitHash == commit.Commit.parentHashes[0] {
			maxDistanceFromParent = math.Max(math.Abs(float64(commits[i+1].X-commit.X)), maxDistanceFromParent)
		}

		commitInfoHeight := getHeightOfCommit(commit)
		posY += int(math.Max(float64(commitInfoHeight+1), maxDistanceFromParent))
	}
}

func logGraph(pager *outputpager.Pager, apr *argparser.ArgParseResults, commitInfos []CommitInfo) {
	commits := mapCommitsWithChildrenAndPosition(commitInfos)
	commitsMap := make(map[string]*CommitInfoWithChildren)
	for _, commit := range commits {
		commitsMap[commit.Commit.commitHash] = commit
	}

	commits, commitsMap = computeColumnEnds(commits, commitsMap)
	expandGraph(commits, 80)

	// Create a 2D slice to represent the graph
	// each element in the graph matrix is a string of length 1 (either "|", "/", "\", "-", or " ")
	maxX, maxY := 0, 0
	for _, commit := range commits {
		if commit.X > maxX {
			maxX = commit.X
		}
		if commit.Y > maxY {
			maxY = commit.Y
		}
	}
	heightOfLastCommit := getHeightOfCommit(commits[len(commits)-1])
	graph := make([][]string, maxY+heightOfLastCommit)
	for i := range graph {
		graph[i] = make([]string, maxX+2)
		for j := range graph[i] {
			graph[i][j] = " "
		}
	}

	// Draw the commits and paths
	for _, commit := range commits {
		x := commit.X
		y := commit.Y
		graph[y][x] = "\033[37m*"

		// draw the path between the commit and its parent
		for _, parentHash := range commit.Commit.parentHashes {
			if parent, ok := commitsMap[parentHash]; ok {
				// the parent is on the same branch/column
				if parent.X == commit.X {
					color := branchColors[x/2%len(branchColors)]
					for yy := commit.Y + 1; yy < parent.Y; yy++ {
						if graph[yy][x] == " " {
							graph[yy][x] = fmt.Sprintf("%s|", color)
						}
					}
				}
				// from parent to the current commit, a new branch path is created
				// the first part is draw a diagonal line from the parent to the column of the current commit
				// the second part is extending the path to the current commit along the y-axis
				if parent.X < commit.X {
					color := branchColors[x/2%len(branchColors)]
					for xx := parent.X + 1; xx < commit.X; xx++ {
						if graph[parent.Y+parent.X+1-xx][xx] == " " {
							graph[parent.Y+parent.X+1-xx][xx] = fmt.Sprintf("%s/", color)
						}
					}
					for yy := parent.Y + parent.X + 1 - commit.X; yy > commit.Y; yy-- {
						if graph[yy][x] == " " {
							graph[yy][x] = fmt.Sprintf("%s|", color)
						}
					}
				}
				// the current commit is a merge commit
				// the first part is draw a diagonal line from the current commit to the column of the parent commit
				// the second part is extending the path to the parent commit along the y-axis
				if parent.X > commit.X {
					color := branchColors[parent.X/2%len(branchColors)]
					for xx := commit.X + 1; xx < parent.X; xx++ {
						if graph[commit.Y+xx-commit.X-1][xx] == " " {
							graph[commit.Y+xx-commit.X-1][xx] = fmt.Sprintf("%s\\", color)
						}
					}
					for yy := commit.Y + parent.X - (commit.X + 1); yy < parent.Y; yy++ {
						if graph[yy][parent.X] == " " {
							graph[yy][parent.X] = fmt.Sprintf("%s|", color)
						}
					}
				}
			}
		}
	}

	// trim the trailing empty space of each line so we can use the length of the line to align the commit message
	for i, line := range graph {
		line = trimTrailing(line)
		graph[i] = line
	}

	appendMessage(graph, pager, apr, commits)
}

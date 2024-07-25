package commands

import (
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/dolt/go/store/util/outputpager"
)

// Define the structure of the commit data
type Commit struct {
	Hash        string
	Parents     []string
	Children    []string
	Committer   string
	Message     string
	CommitDate  string
	CommitColor string
	X           int
	Y           int
}

type CommitInfoWithChildren struct {
	Commit           CommitInfo
	Children         []string
	X                int
	Y                int
	formattedMessage []string
}

var branchColors = []string{
	"\033[31m",   // Red
	"\033[32m",   // Green
	"\033[34m",   // Blue
	"\033[35m",   // Magenta
	"\033[36m",   // Cyan
	"\033[39m",   // Default
	"\033[1;31m", // Bold Red
	"\033[1;32m", // Bold Green
	"\033[1;33m", // Bold Yellow
	"\033[1;34m", // Bold Blue
	"\033[1;35m", // Bold Magenta
	"\033[1;36m", // Bold Cyan
	"\033[1;37m", // Bold White
	"\033[1;39m", // Bold Default
	"\033[2;31m", // Faint Red
	"\033[2;32m", // Faint Green
	"\033[2;33m", // Faint Yellow
	"\033[2;34m", // Faint Blue
	"\033[2;35m", // Faint Magenta
	"\033[2;36m", // Faint Cyan
	"\033[2;37m", // Faint White
	"\033[2;39m", // Faint Default
	"\033[41m",   // Background Red
	"\033[42m",   // Background Green
	"\033[43m",   // Background Yellow
	"\033[44m",   // Background Blue
	"\033[45m",   // Background Magenta
	"\033[46m",   // Background Cyan
	"\033[47m",   // Background White
	"\033[49m",   // Background Default
	"\033[2m",    // Faint
	"\033[2;3m",  // Faint Italic
	"\033[7m",    // Reverse
}

// get the children of commits, and initialize the x and y coordinates of the commits
func formatCommits(commits []CommitInfo) []CommitInfoWithChildren {
	childrenMap := make(map[string][]string)
	for _, commit := range commits {
		for _, parent := range commit.parentHashes {
			childrenMap[parent] = append(childrenMap[parent], commit.commitHash)
		}
	}

	var commitsWithChildren []CommitInfoWithChildren
	for y, commit := range commits {
		commitsWithChildren = append(commitsWithChildren, CommitInfoWithChildren{
			Commit:   commit,
			Children: childrenMap[commit.commitHash],
			X:        -1,
			Y:        y,
		})
	}

	return commitsWithChildren
}

type BranchPathType struct {
	Start         int
	End           int
	EndCommitHash string
	BranchOrder   int
}

func computeColumns(commits []CommitInfoWithChildren) []CommitInfoWithChildren {
	commitsMap := make(map[string]CommitInfoWithChildren)
	for _, commit := range commits {
		commitsMap[commit.Commit.commitHash] = commit
	}

	columns := [][]BranchPathType{}
	commitYs := make(map[string]int)

	commitXs := make(map[string]int)
	for index, commit := range commits {
		commitXs[commit.Commit.commitHash] = index
	}

	updateColumns := func(col, end int, endCommitHash string) {
		columns[col][len(columns[col])-1].End = end
		columns[col][len(columns[col])-1].EndCommitHash = endCommitHash
	}

	branchOrder := 0

	for index, commit := range commits {
		var branchChildren []string
		for _, child := range commit.Children {
			if commitsMap[child].Commit.parentHashes[0] == commit.Commit.commitHash {
				branchChildren = append(branchChildren, child)
			}
		}

		isLastCommitOnBranch := len(commit.Children) == 0
		isChildOfNonMergeCommit := len(branchChildren) > 0

		commitY := -1

		isFirstCommit := len(commit.Commit.parentHashes) == 0

		if isLastCommitOnBranch {
			columns = append(columns, []BranchPathType{
				{
					Start: index,
					End: func() int {
						if isFirstCommit {
							return index
						} else {
							return math.MaxInt
						}
					}(),
					EndCommitHash: commit.Commit.commitHash,
					BranchOrder:   branchOrder,
				},
			})
			branchOrder++
			commitY = len(columns) - 1
		} else if isChildOfNonMergeCommit {
			var branchChildrenYs []int
			for _, childHash := range branchChildren {
				if y, ok := commitYs[childHash]; ok {
					branchChildrenYs = append(branchChildrenYs, y)
				}
			}

			commitY = min(branchChildrenYs...)

			updateColumns(commitY, func() int {
				if isFirstCommit {
					return index
				} else {
					return math.MaxInt
				}
			}(), commit.Commit.commitHash)

			for _, childY := range branchChildrenYs {
				if childY != commitY {
					updateColumns(childY, index-1, commit.Commit.commitHash)
				}
			}
		} else {
			minChildX := math.MaxInt
			maxChildY := -1

			for _, child := range commit.Children {
				childX := commitXs[child]
				childY := commitYs[child]

				if childX < minChildX {
					minChildX = childX
				}

				if childY > maxChildY {
					maxChildY = childY
				}
			}

			colFitAtEnd := -1
			for i := maxChildY + 1; i < len(columns); i++ {
				if minChildX >= columns[i][len(columns[i])-1].End {
					colFitAtEnd = i - (maxChildY + 1)
					break
				}
			}

			col := -1
			if colFitAtEnd != -1 {
				col = maxChildY + 1 + colFitAtEnd
			}

			if col == -1 {
				columns = append(columns, []BranchPathType{
					{
						Start: minChildX + 1,
						End: func() int {
							if isFirstCommit {
								return index
							} else {
								return math.MaxInt
							}
						}(),
						EndCommitHash: commit.Commit.commitHash,
						BranchOrder:   branchOrder,
					},
				})
				branchOrder++
				commitY = len(columns) - 1
			} else {
				commitY = col
				columns[col] = append(columns[col], BranchPathType{
					Start: minChildX + 1,
					End: func() int {
						if isFirstCommit {
							return index
						} else {
							return math.MaxInt
						}
					}(),
					EndCommitHash: commit.Commit.commitHash,
					BranchOrder:   branchOrder,
				})
				branchOrder++
			}
		}

		commitYs[commit.Commit.commitHash] = commitY
		commits[index].X = commitY
	}
	return commits
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

// print the commit message in a constrained width
func wrapTextOnWidth(text string, width int) (int, []string) {
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
	return totalRows, wrappedLines
}

func printCommitMetadata(graph [][]string, graphWithMessage []string, posY, posX int, commit CommitInfoWithChildren) []string {
	firstLine := strings.Join(graph[posY], "")
	emptySpace := strings.Repeat(" ", posX-len(graph[posY]))
	graphWithMessage[posY] = fmt.Sprintf("%s%s\033[33m commit %s", firstLine, emptySpace, commit.Commit.commitHash)

	secondLine := strings.Join(graph[posY+1], "")
	emptySpace = strings.Repeat(" ", posX-len(graph[posY+1]))
	graphWithMessage[posY+1] = fmt.Sprintf("%s%s\033[37m Author: %s", secondLine, emptySpace, commit.Commit.commitMeta.Name)

	thirdLine := strings.Join(graph[posY+2], "")
	emptySpace = strings.Repeat(" ", posX-len(graph[posY+2]))
	graphWithMessage[posY+2] = fmt.Sprintf("%s%s\033[37m Date: %s", thirdLine, emptySpace, commit.Commit.commitMeta.FormatTS())

	fourthLine := strings.Join(graph[posY+3], "")
	graphWithMessage[posY+3] = fourthLine

	return graphWithMessage
}

// print the commit messages in the graph matrix
// the graph is a 2D slice of strings, each element in the graph matrix is a string of length 1 (either "|", "/", "\", "-", or " ")
// the graphWithMessage is a 1D slice of strings, each element is a line of the graph with the commit message appended
func appendMessage(graph [][]string, commits []CommitInfoWithChildren) []string {
	graphWithMessage := make([]string, len(graph))

	// start from the last commit
	last_commit_y := commits[len(commits)-1].Y
	graphWithMessage = printCommitMetadata(graph, graphWithMessage, last_commit_y, len(graph[last_commit_y]), commits[len(commits)-1])

	for i, line := range commits[len(commits)-1].formattedMessage {
		y := last_commit_y + 4 + i
		graphWithMessage[y] = fmt.Sprintf("  \033[37m%s", line)
	}
	for i := len(commits) - 2; i >= 0; i-- {
		startY := commits[i].Y
		endY := commits[i+1].Y
		startX := commits[i].X + 1
		for j := startY; j < endY; j++ {
			if len(graph[j]) > startX {
				startX = len(graph[j])
			}
		}

		graphWithMessage = printCommitMetadata(graph, graphWithMessage, startY, startX, commits[i])

		for i, line := range commits[i].formattedMessage {
			y := startY + 4 + i
			lineContent := strings.Join(graph[y], "")
			emptySpace := strings.Repeat(" ", startX-len(graph[y]))
			graphWithMessage[y] = fmt.Sprintf("%s%s\033[37m %s", lineContent, emptySpace, line)
		}
		for j := startY + 4 + len(commits[i].formattedMessage); j < endY; j++ {
			graphWithMessage[j] = strings.Join(graph[j], "")
		}
	}
	return graphWithMessage
}

func expandGraph(commits []CommitInfoWithChildren, width int) []CommitInfoWithChildren {
	expandedCommits := make([]CommitInfoWithChildren, 0)
	posY := 0
	// Iterate over the commits in the original graph
	for i, commit := range commits {
		commit.X = commit.X * 2
		commit.Y = posY
		rowNum, formattedMessage := wrapTextOnWidth(commit.Commit.commitMeta.Description, width)
		maxDistanceFromParent := float64(0)
		// make sure there is enough space for the diagonal line connecting to the parent
		// this is an approximation, assume that there will be enough space if parent is not the next commit
		if i < len(commits)-1 && commits[i+1].Commit.commitHash == commit.Commit.parentHashes[0] {
			maxDistanceFromParent = math.Max(math.Abs(float64(commits[i+1].X-commit.X)), maxDistanceFromParent)
		}
		posY += int(math.Max(float64(5+rowNum), maxDistanceFromParent))
		commit.formattedMessage = formattedMessage
		expandedCommits = append(expandedCommits, commit)
	}

	return expandedCommits
}

func trimTrailing(row []string) []string {
	lastIndex := len(row) - 1

	// Find the last non-empty string in the row
	for lastIndex >= 0 && strings.TrimSpace(row[lastIndex]) == "" {
		lastIndex--
	}

	// Return the trimmed row
	return row[:lastIndex+1]
}

// the height that a commit will take up in the graph
// 4 lines for commit metadata (commit hash, author, date, and an empty line) + number of lines in the commit message
func getHeightOfCommit(commit CommitInfoWithChildren) int {
	return 4 + len(commit.formattedMessage)
}

func logGraph(pager *outputpager.Pager, commitInfos []CommitInfo) {
	commits := formatCommits(commitInfos)
	commits = computeColumns(commits)

	commits = expandGraph(commits, 80)
	// Determine the width and height of the graph matrix
	maxX, maxY := 0, 0
	for _, commit := range commits {
		if commit.X > maxX {
			maxX = commit.X
		}
		if commit.Y > maxY {
			maxY = commit.Y
		}
	}

	// Create a 2D slice to represent the graph
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

		for _, parentHash := range commit.Commit.parentHashes {
			for _, parent := range commits {
				if parent.Commit.commitHash == parentHash {
					if parent.X == commit.X {
						for yy := commit.Y + 1; yy < parent.Y; yy++ {
							if graph[yy][x] == " " {
								graph[yy][x] = fmt.Sprintf("%s|", branchColors[x/2])
							}
						}
					}
					if parent.X < commit.X {
						for xx := parent.X + 1; xx < commit.X; xx++ {
							if graph[parent.Y+parent.X+1-xx][xx] == " " {
								graph[parent.Y+parent.X+1-xx][xx] = fmt.Sprintf("%s/", branchColors[x/2])
							}
						}
						for yy := parent.Y + parent.X + 1 - commit.X; yy > commit.Y; yy-- {
							if graph[yy][x] == " " {
								graph[yy][x] = fmt.Sprintf("%s|", branchColors[x/2])
							}
						}
					}
					if parent.X > commit.X {
						for xx := commit.X + 1; xx < parent.X; xx++ {
							if graph[commit.Y+xx-commit.X-1][xx] == " " {
								graph[commit.Y+xx-commit.X-1][xx] = fmt.Sprintf("%s\\", branchColors[parent.X/2])
							}
						}
						for yy := commit.Y + parent.X - (commit.X + 1); yy < parent.Y; yy++ {
							if graph[yy][parent.X] == " " {
								graph[yy][parent.X] = fmt.Sprintf("%s|", branchColors[parent.X/2])
							}
						}
					}
				}
			}
		}
	}

	for i, line := range graph {
		line = trimTrailing(line)
		graph[i] = line
	}

	graphWithMessage := appendMessage(graph, commits)
	for _, line := range graphWithMessage {
		pager.Writer.Write([]byte(line))
		pager.Writer.Write([]byte("\n"))
	}

}

package enginetest

import (
	"bufio"
	"strings"
)

/*
	   pmd parses merge docs which have the following syntax.

		ancestor:
			[SQL script]

		right:
			[SQL script]

		left:
			[SQL script]

		It will output SQL commands that will build ancestor, left, and right commits that can be merged.
*/
func pmd(str string) []string {
	var output []string
	scan := bufio.NewScanner(strings.NewReader(str))

	state := 0
	for scan.Scan() {

		line := strings.TrimSpace(scan.Text())

		if len(line) == 0 {
			// skip empty lines
			continue
		}

		switch state {
		// check for ancestor label state
		case 0:
			if line != "ancestor:" {
				panic("missing ancestor: label")
			}
			state = 1
		// parse ancestor part
		case 1:
			if line == "right:" {
				state = 2
				output = append(output,
					"CALL DOLT_COMMIT('-Am', 'ancestor commit');",
					"CALL DOLT_CHECKOUT('-b', 'right');")
				continue
			}
			output = append(output, line)
		// parse right part
		case 2:
			if line == "left:" {
				state = 3
				output = append(output,
					"CALL DOLT_COMMIT('-Am', 'right commit');",
					"CALL DOLT_CHECKOUT('main');")
				continue
			}
			output = append(output, line)
		// parse left part
		case 3:
			output = append(output, line)
		}
	}

	if state == 1 {
		panic("missing right: label")
	} else if state == 2 {
		panic("missing left: label")
	}

	output = append(output, "CALL DOLT_COMMIT('-Am', 'left commit');")

	return output
}

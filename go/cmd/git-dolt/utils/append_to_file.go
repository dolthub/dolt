package utils

import (
	"fmt"
	"os"
)

// AppendToFile appends the given string to the given file in the current
// directory, creating it if it does not exist.
func AppendToFile(f string, s string) error {
	giFile, err := os.OpenFile(f, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening %s: %v", f, err)
	}
	defer giFile.Close()

	if _, err = giFile.WriteString(fmt.Sprintf("%s\n", s)); err != nil {
		return fmt.Errorf("error writing to %s at %v", f, err)
	}

	return nil
}

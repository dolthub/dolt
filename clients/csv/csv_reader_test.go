package csv

import (
    "testing"
    "bytes"
    "encoding/csv"
    // "fmt"
    "strings"
    "bufio"
)


func TestCR(t *testing.T) {
    testFile := bytes.NewBufferString("a,b,c\r1,2,3\r").Bytes()

    r := csv.NewReader(SafeCSVReader(bufio.NewReader(bytes.NewReader(testFile))))
    lines, err := r.ReadAll()

    if err != nil {
        t.Errorf("An error occurred while reading the data: %v", err)
    }
    if len(lines) != 2 {
        t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
    }
}

func TestLF(t *testing.T) {
    testFile := bytes.NewBufferString("a,b,c\n1,2,3\n").Bytes()

    r := csv.NewReader(SafeCSVReader(bufio.NewReader(bytes.NewReader(testFile))))
    lines, err := r.ReadAll()

    if err != nil {
        t.Errorf("An error occurred while reading the data: %v", err)
    }
    if len(lines) != 2 {
        t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
    }
}

func TestCRLF(t *testing.T) {
    testFile := bytes.NewBufferString("a,b,c\r\n1,2,3\r\n").Bytes()

    r := csv.NewReader(SafeCSVReader(bufio.NewReader(bytes.NewReader(testFile))))
    lines, err := r.ReadAll()

    if err != nil {
        t.Errorf("An error occurred while reading the data: %v", err)
    }
    if len(lines) != 2 {
        t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
    }
}

func TestCRInQuote(t *testing.T) {
    testFile := bytes.NewBufferString("a,\"foo,\rbar\",c\r1,\"2\r\n2\",3\r").Bytes()

    r := csv.NewReader(SafeCSVReader(bufio.NewReader(bytes.NewReader(testFile))))
    lines, err := r.ReadAll()

    if err != nil {
        t.Errorf("An error occurred while reading the data: %v", err)
    }
    if len(lines) != 2 {
        t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
    }
    if strings.Contains(lines[1][1], "\n\n") {
        t.Error("The CRLF was converted to a LFLF")
    }
}

func TestCRLFEndOfBufferLength(t *testing.T) {
    testFile := bytes.NewBuffer(make([]byte, 4096 * 2, 4096 * 2)).Bytes()
    testFile[4095] = 13 // \r byte
    testFile[4096] = 10 // \n byte

    r := csv.NewReader(SafeCSVReader(bufio.NewReader(bytes.NewReader(testFile))))
    lines, err := r.ReadAll()

    if err != nil {
        t.Errorf("An error occurred while reading the data: %v", err)
    }
    if len(lines) != 2 {
        t.Errorf("Wrong number of lines. Expected 2, got %d", len(lines))
    }
}

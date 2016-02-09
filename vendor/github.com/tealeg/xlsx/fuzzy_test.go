// +build fuzzy

package xlsx

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	. "gopkg.in/check.v1"
)

type Fuzzy struct{}

var _ = Suite(&Fuzzy{})
var randseed *int64 = flag.Int64("test.seed", time.Now().Unix(), "Set the random seed of the test for repeatable results")

type tokenchange struct {
	file bytes.Buffer
	old  xml.Token
	new  xml.Token
}

type filechange struct {
	File *zip.Reader
	Name string
	Old  xml.Token
	New  xml.Token
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var numbers = []rune("0123456789")

func randString(n int) []byte {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return []byte(string(b))
}

func randInt(n int) []byte {
	b := make([]rune, n)
	for i := range b {
		b[i] = numbers[rand.Intn(len(numbers))]
	}
	return []byte(string(b))
}

//This function creates variations on tokens without regards as to positions in the file.
func getTokenVariations(t xml.Token) []xml.Token {
	var result []xml.Token = make([]xml.Token, 0)
	switch t := t.(type) {
	case xml.CharData:
		{
			//If the token is a number try some random number
			if _, err := strconv.Atoi(string(t)); err == nil {
				result = append(result, xml.CharData(randInt(rand.Intn(15))))
			}

			result = append(result, xml.CharData(randString(rand.Intn(100))))
			return result
		}
	case xml.StartElement:
		{
			for k := range t.Attr {
				if _, err := strconv.Atoi(string(t.Attr[k].Value)); err == nil {
					start := xml.CopyToken(t).(xml.StartElement)
					start.Attr[k].Value = string(randInt(rand.Intn(15)))
					result = append(result, start)
				}
				start := xml.CopyToken(t).(xml.StartElement)
				start.Attr[k].Value = string(randString(rand.Intn(100)))
				result = append(result, start)
			}
			return result
		}

	default:
		{
			return make([]xml.Token, 0) // No variations on non char tokens yet
		}
	}
}

func variationsXML(f *zip.File) chan tokenchange {
	result := make(chan tokenchange)
	r, _ := f.Open()
	xmlReader := xml.NewDecoder(r)
	var tokenList []xml.Token
	for {
		if t, err := xmlReader.Token(); err == nil {
			tokenList = append(tokenList, xml.CopyToken(t))
		} else {
			break
		}
	}

	go func() {
		//Over every token we want to break
		for TokenToBreak, _ := range tokenList {
			//Get the ways we can break that token
			for _, brokenToken := range getTokenVariations(tokenList[TokenToBreak]) {
				var buf bytes.Buffer
				xmlWriter := xml.NewEncoder(&buf)
				//Now create an xml file where one token is broken
				for currentToken, t := range tokenList {
					if currentToken == TokenToBreak {
						xmlWriter.EncodeToken(brokenToken)
					} else {
						xmlWriter.EncodeToken(t)
					}
				}
				xmlWriter.Flush()
				result <- tokenchange{buf, tokenList[TokenToBreak], brokenToken}
			}
		}
		close(result)
	}()
	return result
}

func generateBrokenFiles(r *zip.Reader) chan filechange {
	result := make(chan filechange)
	go func() {
		count := 0
		//For every file in the zip we want variation on
		for breakIndex, fileToBreak := range r.File {
			if filepath.Ext(fileToBreak.Name) != ".xml" {
				continue //We cannot create variations on non-xml files
			}

			variationCount := 0
			//For every broken version of that file
			for changedFile := range variationsXML(fileToBreak) {
				variationCount++
				var buffer bytes.Buffer
				//Create a new xlsx file in memory
				outZip := zip.NewWriter(&buffer)
				w, err := outZip.Create(fileToBreak.Name)
				if err != nil {
					log.Fatal(err)
				}
				//Add modified file to xlsx
				_, err = changedFile.file.WriteTo(w)
				if err != nil {
					log.Fatal("changedFile.file.WriteTo", err)
				}
				//Add other, unchanged, files.
				for otherIndex, otherFile := range r.File {
					if breakIndex == otherIndex {
						continue
					}
					to, err := outZip.Create(otherFile.Name)
					if err != nil {
						log.Fatal("Could not add new file to xlsx due to", err)
					}
					from, err := otherFile.Open()
					if err != nil {
						log.Fatal("Could not open original file from template xlsx due to", err)
					}
					io.Copy(to, from)
					from.Close()
				}
				outZip.Close()

				//Return this combination of broken files
				b := buffer.Bytes()
				var res filechange
				res.File, _ = zip.NewReader(bytes.NewReader(b), int64(len(b)))
				res.Name = fileToBreak.Name
				res.Old = changedFile.old
				res.New = changedFile.new
				result <- res
				count++
			}
		}
		close(result)
	}()
	return result
}

func Raises(f func()) (err interface{}) {
	defer func() {
		err = recover()
	}()
	err = nil
	f()
	return
}

func tokenToString(t xml.Token) string {
	switch t := t.(type) {
	case xml.CharData:
		{
			return string(t)
		}
	default:
		{
			return fmt.Sprint(t)
		}
	}
}

func (f *Fuzzy) TestRandomBrokenParts(c *C) {
	if testing.Short() {
		c.Log("This test, tests many versions of an xlsx file and might take a while, it is being skipped")
		c.SucceedNow()
	}
	log.Println("Fuzzy test is using this -test.seed=" + strconv.FormatInt(*randseed, 10))
	rand.Seed(*randseed)
	template, err := zip.OpenReader("./testdocs/testfile.xlsx")
	c.Assert(err, IsNil)
	defer template.Close()

	count := 0

	for brokenFile := range generateBrokenFiles(&template.Reader) {
		count++
		if testing.Verbose() {
			//If the library panics fatally it would be nice to know why
			log.Println("Testing change to ", brokenFile.Name, " on token ", tokenToString(brokenFile.Old), " of type ", reflect.TypeOf(brokenFile.Old), " to ", tokenToString(brokenFile.New))
		}

		if e := Raises(func() { ReadZipReader(brokenFile.File) }); e != nil {

			c.Log("Some file with random changes did raise an exception instead of returning an error", e)
			c.Log("Testing change to ", brokenFile.Name, " on token ", tokenToString(brokenFile.Old), " of type ", reflect.TypeOf(brokenFile.Old), " to ", tokenToString(brokenFile.New))
			c.FailNow()
		}

	}
	c.Succeed()
}

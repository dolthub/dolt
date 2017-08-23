/*
Copyright (c) 2014 Brendan McMillion

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package proquint

import (
    "bytes"
    "strings"
    "regexp"
)

var (
    conse = [...]byte{'b', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 
        'p', 'r', 's', 't', 'v', 'z'}
    vowse = [...]byte{'a', 'i', 'o', 'u'}
    
    consd = map[byte] uint16 {
        'b' :  0, 'd' :  1, 'f' :  2, 'g' :  3,
        'h' :  4, 'j' :  5, 'k' :  6, 'l' :  7,
        'm' :  8, 'n' :  9, 'p' : 10, 'r' : 11,
        's' : 12, 't' : 13, 'v' : 14, 'z' : 15,
    }
    
    vowsd = map[byte] uint16 {
        'a' : 0, 'i' : 1, 'o' : 2, 'u' : 3,
    }
)

/**
* Tests if a given string is a Proquint identifier
*
* @param {string} str The candidate string.
*
* @return {bool} Whether or not it qualifies.
* @return {error} Error
*/
func IsProquint(str string) (bool, error) {
    exp := "^([abdfghijklmnoprstuvz]{5}-)*[abdfghijklmnoprstuvz]{5}$"
    ok, err := regexp.MatchString(exp, str)
    
    return ok, err
}

/**
* Encodes an arbitrary byte slice into an identifier.
*
* @param {[]byte} buf Slice of bytes to encode.
*
* @return {string} The given byte slice as an identifier.
*/
func Encode(buf []byte) string {
    var out bytes.Buffer
    
    for i := 0; i < len(buf); i = i + 2 {
        var n uint16 = (uint16(buf[i]) * 256) + uint16(buf[i + 1])
        
        var (
            c1 = n         & 0x0f
            v1 = (n >> 4)  & 0x03
            c2 = (n >> 6)  & 0x0f
            v2 = (n >> 10) & 0x03
            c3 = (n >> 12) & 0x0f
        )
        
        out.WriteByte(conse[c1])
        out.WriteByte(vowse[v1])
        out.WriteByte(conse[c2])
        out.WriteByte(vowse[v2])
        out.WriteByte(conse[c3])
        
        if (i + 2) < len(buf) {
            out.WriteByte('-')
        }
    }
    
    return out.String()
}

/**
* Decodes an identifier into its corresponding byte slice.
*
* @param {string} str Identifier to convert.
*
* @return {[]byte} The identifier as a byte slice.
*/
func Decode(str string) []byte {
    var (
        out bytes.Buffer
        bits []string = strings.Split(str, "-")
    )
    
    for i := 0; i < len(bits); i++ {
        var x uint16 = consd[bits[i][0]] +
                (vowsd[bits[i][1]] <<  4) +
                (consd[bits[i][2]] <<  6) +
                (vowsd[bits[i][3]] << 10) + 
                (consd[bits[i][4]] << 12)
        
        out.WriteByte(byte(x >> 8))
        out.WriteByte(byte(x))
    }
    
    return out.Bytes()
}

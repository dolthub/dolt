package lexer

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	"github.com/attic-labs/graphql/gqlerrors"
	"github.com/attic-labs/graphql/language/source"
)

const (
	EOF = iota + 1
	BANG
	DOLLAR
	PAREN_L
	PAREN_R
	SPREAD
	COLON
	EQUALS
	AT
	BRACKET_L
	BRACKET_R
	BRACE_L
	PIPE
	BRACE_R
	NAME
	INT
	FLOAT
	STRING
)

var TokenKind map[int]int
var tokenDescription map[int]string

func init() {
	TokenKind = make(map[int]int)
	tokenDescription = make(map[int]string)
	TokenKind[EOF] = EOF
	TokenKind[BANG] = BANG
	TokenKind[DOLLAR] = DOLLAR
	TokenKind[PAREN_L] = PAREN_L
	TokenKind[PAREN_R] = PAREN_R
	TokenKind[SPREAD] = SPREAD
	TokenKind[COLON] = COLON
	TokenKind[EQUALS] = EQUALS
	TokenKind[AT] = AT
	TokenKind[BRACKET_L] = BRACKET_L
	TokenKind[BRACKET_R] = BRACKET_R
	TokenKind[BRACE_L] = BRACE_L
	TokenKind[PIPE] = PIPE
	TokenKind[BRACE_R] = BRACE_R
	TokenKind[NAME] = NAME
	TokenKind[INT] = INT
	TokenKind[FLOAT] = FLOAT
	TokenKind[STRING] = STRING
	tokenDescription[TokenKind[EOF]] = "EOF"
	tokenDescription[TokenKind[BANG]] = "!"
	tokenDescription[TokenKind[DOLLAR]] = "$"
	tokenDescription[TokenKind[PAREN_L]] = "("
	tokenDescription[TokenKind[PAREN_R]] = ")"
	tokenDescription[TokenKind[SPREAD]] = "..."
	tokenDescription[TokenKind[COLON]] = ":"
	tokenDescription[TokenKind[EQUALS]] = "="
	tokenDescription[TokenKind[AT]] = "@"
	tokenDescription[TokenKind[BRACKET_L]] = "["
	tokenDescription[TokenKind[BRACKET_R]] = "]"
	tokenDescription[TokenKind[BRACE_L]] = "{"
	tokenDescription[TokenKind[PIPE]] = "|"
	tokenDescription[TokenKind[BRACE_R]] = "}"
	tokenDescription[TokenKind[NAME]] = "Name"
	tokenDescription[TokenKind[INT]] = "Int"
	tokenDescription[TokenKind[FLOAT]] = "Float"
	tokenDescription[TokenKind[STRING]] = "String"
}

// Token is a representation of a lexed Token. Value only appears for non-punctuation
// tokens: NAME, INT, FLOAT, and STRING.
type Token struct {
	Kind  int
	Start int
	End   int
	Value string
}

type Lexer func(resetPosition int) (Token, error)

func Lex(s *source.Source) Lexer {
	var prevPosition int
	return func(resetPosition int) (Token, error) {
		if resetPosition == 0 {
			resetPosition = prevPosition
		}
		token, err := readToken(s, resetPosition)
		if err != nil {
			return token, err
		}
		prevPosition = token.End
		return token, nil
	}
}

// Reads an alphanumeric + underscore name from the source.
// [_A-Za-z][_0-9A-Za-z]*
// position: Points to the byte position in the byte array
// runePosition: Points to the rune position in the byte array
func readName(source *source.Source, position, runePosition int) Token {
	body := source.Body
	bodyLength := len(body)
	endByte := position + 1
	endRune := runePosition + 1
	for {
		code, _ := runeAt(body, endByte)
		if (endByte != bodyLength) &&
			(code == '_' || // _
				code >= '0' && code <= '9' || // 0-9
				code >= 'A' && code <= 'Z' || // A-Z
				code >= 'a' && code <= 'z') { // a-z
			endByte++
			endRune++
			continue
		} else {
			break
		}
	}
	return makeToken(TokenKind[NAME], runePosition, endRune, string(body[position:endByte]))
}

// Reads a number token from the source file, either a float
// or an int depending on whether a decimal point appears.
// Int:   -?(0|[1-9][0-9]*)
// Float: -?(0|[1-9][0-9]*)(\.[0-9]+)?((E|e)(+|-)?[0-9]+)?
func readNumber(s *source.Source, start int, firstCode rune, codeLength int) (Token, error) {
	code := firstCode
	body := s.Body
	position := start
	isFloat := false
	if code == '-' { // -
		position += codeLength
		code, codeLength = runeAt(body, position)
	}
	if code == '0' { // 0
		position += codeLength
		code, codeLength = runeAt(body, position)
		if code >= '0' && code <= '9' {
			description := fmt.Sprintf("Invalid number, unexpected digit after 0: %v.", printCharCode(code))
			return Token{}, gqlerrors.NewSyntaxError(s, position, description)
		}
	} else {
		p, err := readDigits(s, position, code, codeLength)
		if err != nil {
			return Token{}, err
		}
		position = p
		code, codeLength = runeAt(body, position)
	}
	if code == '.' { // .
		isFloat = true
		position += codeLength
		code, codeLength = runeAt(body, position)
		p, err := readDigits(s, position, code, codeLength)
		if err != nil {
			return Token{}, err
		}
		position = p
		code, codeLength = runeAt(body, position)
	}
	if code == 'E' || code == 'e' { // E e
		isFloat = true
		position += codeLength
		code, codeLength = runeAt(body, position)
		if code == '+' || code == '-' { // + -
			position += codeLength
			code, codeLength = runeAt(body, position)
		}
		p, err := readDigits(s, position, code, codeLength)
		if err != nil {
			return Token{}, err
		}
		position = p
	}
	kind := TokenKind[INT]
	if isFloat {
		kind = TokenKind[FLOAT]
	}

	return makeToken(kind, start, position, string(body[start:position])), nil
}

// Returns the new position in the source after reading digits.
func readDigits(s *source.Source, start int, firstCode rune, codeLength int) (int, error) {
	body := s.Body
	position := start
	code := firstCode
	if code >= '0' && code <= '9' { // 0 - 9
		for {
			if code >= '0' && code <= '9' { // 0 - 9
				position += codeLength
				code, codeLength = runeAt(body, position)
				continue
			} else {
				break
			}
		}
		return position, nil
	}
	var description string
	description = fmt.Sprintf("Invalid number, expected digit but got: %v.", printCharCode(code))
	return position, gqlerrors.NewSyntaxError(s, position, description)
}

func readString(s *source.Source, start int) (Token, error) {
	body := s.Body
	position := start + 1
	runePosition := start + 1
	chunkStart := position
	var code rune
	var n int
	var valueBuffer bytes.Buffer
	for {
		code, n = runeAt(body, position)
		if position < len(body) &&
			// not LineTerminator
			code != 0x000A && code != 0x000D &&
			// not Quote (")
			code != '"' {

			// SourceCharacter
			if code < 0x0020 && code != 0x0009 {
				return Token{}, gqlerrors.NewSyntaxError(s, runePosition, fmt.Sprintf(`Invalid character within String: %v.`, printCharCode(code)))
			}
			position += n
			runePosition++
			if code == '\\' { // \
				valueBuffer.Write(body[chunkStart : position-1])
				code, n = runeAt(body, position)
				switch code {
				case '"':
					valueBuffer.WriteRune('"')
					break
				case '/':
					valueBuffer.WriteRune('/')
					break
				case '\\':
					valueBuffer.WriteRune('\\')
					break
				case 'b':
					valueBuffer.WriteRune('\b')
					break
				case 'f':
					valueBuffer.WriteRune('\f')
					break
				case 'n':
					valueBuffer.WriteRune('\n')
					break
				case 'r':
					valueBuffer.WriteRune('\r')
					break
				case 't':
					valueBuffer.WriteRune('\t')
					break
				case 'u':
					// Check if there are at least 4 bytes available
					if len(body) <= position+4 {
						return Token{}, gqlerrors.NewSyntaxError(s, runePosition,
							fmt.Sprintf("Invalid character escape sequence: "+
								"\\u%v", string(body[position+1:])))
					}
					charCode := uniCharCode(
						rune(body[position+1]),
						rune(body[position+2]),
						rune(body[position+3]),
						rune(body[position+4]),
					)
					if charCode < 0 {
						return Token{}, gqlerrors.NewSyntaxError(s, runePosition,
							fmt.Sprintf("Invalid character escape sequence: "+
								"\\u%v", string(body[position+1:position+5])))
					}
					valueBuffer.WriteRune(charCode)
					position += 4
					runePosition += 4
					break
				default:
					return Token{}, gqlerrors.NewSyntaxError(s, runePosition,
						fmt.Sprintf(`Invalid character escape sequence: \\%c.`, code))
				}
				position += n
				runePosition++
				chunkStart = position
			}
			continue
		} else {
			break
		}
	}
	if code != '"' { // quote (")
		return Token{}, gqlerrors.NewSyntaxError(s, runePosition, "Unterminated string.")
	}
	stringContent := body[chunkStart:position]
	valueBuffer.Write(stringContent)
	value := valueBuffer.String()
	return makeToken(TokenKind[STRING], start, position+1, value), nil
}

// Converts four hexidecimal chars to the integer that the
// string represents. For example, uniCharCode('0','0','0','f')
// will return 15, and uniCharCode('0','0','f','f') returns 255.
// Returns a negative number on error, if a char was invalid.
// This is implemented by noting that char2hex() returns -1 on error,
// which means the result of ORing the char2hex() will also be negative.
func uniCharCode(a, b, c, d rune) rune {
	return rune(char2hex(a)<<12 | char2hex(b)<<8 | char2hex(c)<<4 | char2hex(d))
}

// Converts a hex character to its integer value.
// '0' becomes 0, '9' becomes 9
// 'A' becomes 10, 'F' becomes 15
// 'a' becomes 10, 'f' becomes 15
// Returns -1 on error.
func char2hex(a rune) int {
	if a >= 48 && a <= 57 { // 0-9
		return int(a) - 48
	} else if a >= 65 && a <= 70 { // A-F
		return int(a) - 55
	} else if a >= 97 && a <= 102 {
		// a-f
		return int(a) - 87
	}
	return -1
}

func makeToken(kind int, start int, end int, value string) Token {
	return Token{Kind: kind, Start: start, End: end, Value: value}
}

func printCharCode(code rune) string {
	// NaN/undefined represents access beyond the end of the file.
	if code < 0 {
		return "<EOF>"
	}
	// print as ASCII for printable range
	if code >= 0x0020 && code < 0x007F {
		return fmt.Sprintf(`"%c"`, code)
	}
	// Otherwise print the escaped form. e.g. `"\\u0007"`
	return fmt.Sprintf(`"\\u%04X"`, code)
}

func readToken(s *source.Source, fromPosition int) (Token, error) {
	body := s.Body
	bodyLength := len(body)
	position, runePosition := positionAfterWhitespace(body, fromPosition)
	if position >= bodyLength {
		return makeToken(TokenKind[EOF], position, position, ""), nil
	}
	code, codeLength := runeAt(body, position)

	// SourceCharacter
	if code < 0x0020 && code != 0x0009 && code != 0x000A && code != 0x000D {
		return Token{}, gqlerrors.NewSyntaxError(s, runePosition, fmt.Sprintf(`Invalid character %v`, printCharCode(code)))
	}

	switch code {
	// !
	case '!':
		return makeToken(TokenKind[BANG], position, position+1, ""), nil
	// $
	case '$':
		return makeToken(TokenKind[DOLLAR], position, position+1, ""), nil
	// (
	case '(':
		return makeToken(TokenKind[PAREN_L], position, position+1, ""), nil
	// )
	case ')':
		return makeToken(TokenKind[PAREN_R], position, position+1, ""), nil
	// .
	case '.':
		next1, _ := runeAt(body, position+1)
		next2, _ := runeAt(body, position+2)
		if next1 == '.' && next2 == '.' {
			return makeToken(TokenKind[SPREAD], position, position+3, ""), nil
		}
		break
	// :
	case ':':
		return makeToken(TokenKind[COLON], position, position+1, ""), nil
	// =
	case '=':
		return makeToken(TokenKind[EQUALS], position, position+1, ""), nil
	// @
	case '@':
		return makeToken(TokenKind[AT], position, position+1, ""), nil
	// [
	case '[':
		return makeToken(TokenKind[BRACKET_L], position, position+1, ""), nil
	// ]
	case ']':
		return makeToken(TokenKind[BRACKET_R], position, position+1, ""), nil
	// {
	case '{':
		return makeToken(TokenKind[BRACE_L], position, position+1, ""), nil
	// |
	case '|':
		return makeToken(TokenKind[PIPE], position, position+1, ""), nil
	// }
	case '}':
		return makeToken(TokenKind[BRACE_R], position, position+1, ""), nil
	// A-Z
	case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
		'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
		return readName(s, position, runePosition), nil
	// _
	// a-z
	case '_', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
		'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z':
		return readName(s, position, runePosition), nil
	// -
	// 0-9
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		token, err := readNumber(s, position, code, codeLength)
		if err != nil {
			return token, err
		}
		return token, nil
	// "
	case '"':
		token, err := readString(s, position)
		if err != nil {
			return token, err
		}
		return token, nil
	}
	description := fmt.Sprintf("Unexpected character %v.", printCharCode(code))
	return Token{}, gqlerrors.NewSyntaxError(s, runePosition, description)
}

// Gets the rune from the byte array at given byte position and it's width in bytes
func runeAt(body []byte, position int) (code rune, charWidth int) {
	if len(body) <= position {
		// <EOF>
		return -1, utf8.RuneError
	}

	c := body[position]
	if c < utf8.RuneSelf {
		return rune(c), 1
	}

	r, n := utf8.DecodeRune(body[position:])
	return r, n
}

// Reads from body starting at startPosition until it finds a non-whitespace
// or commented character, then returns the position of that character for lexing.
// lexing.
// Returns both byte positions and rune position
func positionAfterWhitespace(body []byte, startPosition int) (position int, runePosition int) {
	bodyLength := len(body)
	position = startPosition
	runePosition = startPosition
	for {
		if position < bodyLength {
			code, n := runeAt(body, position)

			// Skip Ignored
			if code == 0xFEFF || // BOM
				// White Space
				code == 0x0009 || // tab
				code == 0x0020 || // space
				// Line Terminator
				code == 0x000A || // new line
				code == 0x000D || // carriage return
				// Comma
				code == 0x002C {
				position += n
				runePosition++
			} else if code == 35 { // #
				position += n
				runePosition++
				for {
					code, n := runeAt(body, position)
					if position < bodyLength &&
						code != 0 &&
						// SourceCharacter but not LineTerminator
						(code > 0x001F || code == 0x0009) && code != 0x000A && code != 0x000D {
						position += n
						runePosition++
						continue
					} else {
						break
					}
				}
			} else {
				break
			}
			continue
		} else {
			break
		}
	}
	return position, runePosition
}

func GetTokenDesc(token Token) string {
	if token.Value == "" {
		return GetTokenKindDesc(token.Kind)
	}
	return fmt.Sprintf("%s \"%s\"", GetTokenKindDesc(token.Kind), token.Value)
}

func GetTokenKindDesc(kind int) string {
	return tokenDescription[kind]
}

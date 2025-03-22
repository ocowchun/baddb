package lexer

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/ocowchun/baddb/expression/token"
	"io"
	"unicode"
)

type Lexer struct {
	scanner      *bufio.Scanner
	position     int
	readPosition int
	ch           rune
	currentLine  []rune
	isEOF        bool
}

func New(input io.Reader) *Lexer {
	l := &Lexer{
		scanner:      bufio.NewScanner(input),
		position:     0,
		readPosition: 0,
		currentLine:  []rune{},
		isEOF:        false,
	}
	l.readRune()
	return l
}

func (l *Lexer) peekRune() (rune, bool) {
	if l.readPosition >= len(l.currentLine) {
		return 0, false
	}
	return l.currentLine[l.readPosition], true
}

func (l *Lexer) readRune() {
	for l.readPosition >= len(l.currentLine) {
		if l.scanner.Scan() {
			l.currentLine = []rune(l.scanner.Text())
			l.position = 0
			l.readPosition = 0
		} else {
			l.isEOF = true
			return
		}
	}

	l.ch = l.currentLine[l.readPosition]
	l.position = l.readPosition
	l.readPosition += 1
}

func newToken(tokenType token.TokenType, literal string) token.Token {
	return token.Token{Type: tokenType, Literal: literal}
}

func (l *Lexer) skipWhitespace() {
	for !l.isEOF && unicode.IsSpace(l.ch) {
		l.readRune()
		if l.isEOF {
			return
		}
	}
}

func (l *Lexer) NextToken() token.Token {
	l.skipWhitespace()
	var tok token.Token
	if l.isEOF {
		return newToken(token.EOF, "")
	}

	switch l.ch {
	case '=':
		tok = newToken(token.EQ, "=")
	case '(':
		tok = newToken(token.LPAREN, "(")
	case ')':
		tok = newToken(token.RPAREN, ")")
	case '[':
		tok = newToken(token.LBRACKET, "[")
	case ']':
		tok = newToken(token.RBRACKET, "]")
	case ',':
		tok = newToken(token.COMMA, ",")
	case '<':
		tok = newToken(token.LT, "<")
	case '>':
		tok = newToken(token.GT, ">")
	case '.':
		tok = newToken(token.DOT, ".")
	case ':':
		tok = newToken(token.COLON, ":")
	case '#':
		tok = newToken(token.SHARP, "#")
	case '"':
		tok.Type = token.STRING
		literal, err := l.readString()
		if err != nil {
			// TODO: improve error handling
			panic(fmt.Sprintf("failed to read string %s", err))
		}
		tok.Literal = literal

	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = token.LookupIdent(tok.Literal)
		} else if isDigit(l.ch) {
			tok = newToken(token.INT, l.readNumber())
		} else {
			tok = newToken(token.ILLEGAL, string(l.ch))
		}
		return tok
	}

	l.readRune()
	return tok
}

func (l *Lexer) readString() (string, error) {
	l.readRune()
	var output bytes.Buffer

	for {
		if l.isEOF {
			return "", io.EOF
		}

		// skip character
		if l.ch == '\\' {
			prev := l.ch
			l.readRune()

			if l.isEOF {
				return "", io.EOF
			}

			if l.ch != '"' {
				output.WriteRune(prev)
			}
		} else if l.ch == '"' {
			break
		}

		output.WriteRune(l.ch)
		l.readRune()
	}

	return output.String(), nil
}

func isDigit(r rune) bool {
	zero := '0'
	nine := '9'

	return r >= zero && r <= nine
}

func (l *Lexer) readNumber() string {
	var output bytes.Buffer
	for isDigit(l.ch) {
		output.WriteRune(l.ch)
		l.readRune()
		if l.isEOF {
			break
		}
	}
	return output.String()
}

func isLetter(r rune) bool {
	// TODO: might need to adjust isLetter logic
	return unicode.IsLetter(r) || string(r) == "_"
}

func (l *Lexer) readIdentifier() string {
	var output bytes.Buffer
	for isLetter(l.ch) || isDigit(l.ch) {
		output.WriteRune(l.ch)
		l.readRune()
		if l.isEOF {
			break
		}
	}
	return output.String()
}

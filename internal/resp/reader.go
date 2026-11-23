// Package resp implements the REdis Serialization Protocol (RESP2).
//
// The reader is zero-dependency and allocation-conscious: command arguments
// are returned as byte slices, and both multibulk (`*N\r\n$len\r\n...`) and
// inline (`SET a b\r\n`, as typed in telnet) request formats are supported.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	MaxBulkLen      = 512 * 1024 * 1024 // 512MB, same as Redis proto-max-bulk-len
	MaxMultiBulkLen = 1024 * 1024       // max arguments per command
	maxInlineLen    = 64 * 1024
)

// ProtocolError is returned for malformed input. The server replies with
// "-ERR Protocol error: ..." and closes the connection, like Redis.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.Msg }

func protoErr(format string, a ...any) error {
	return &ProtocolError{Msg: fmt.Sprintf(format, a...)}
}

// Value is a decoded RESP value (used by clients, tests and the dashboard).
type Value struct {
	Type  byte // '+', '-', ':', '$', '*'
	Str   string
	Int   int64
	Array []Value
	Null  bool
}

// Reader decodes RESP from a stream.
type Reader struct {
	br *bufio.Reader
	cr *countingReader
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func NewReader(r io.Reader) *Reader {
	return NewReaderSize(r, 16*1024)
}

func NewReaderSize(r io.Reader, size int) *Reader {
	cr := &countingReader{r: r}
	return &Reader{br: bufio.NewReaderSize(cr, size), cr: cr}
}

// Buffered reports how many bytes are already buffered. The server uses
// this to detect the end of a pipelined batch, so it can flush replies once
// per batch instead of once per command.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// Offset returns the number of bytes consumed so far (used to truncate a
// partially written AOF tail).
func (r *Reader) Offset() int64 { return r.cr.n - int64(r.br.Buffered()) }

// HasCompleteCommand reports whether the buffered bytes contain at least one
// full request, so the server can parse a pipelined batch without ever
// blocking on the network mid-batch. It performs no I/O.
func (r *Reader) HasCompleteCommand() bool {
	b, _ := r.br.Peek(r.br.Buffered())
	line := func() ([]byte, bool) {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			return nil, false
		}
		l := b[:i]
		b = b[i+1:]
		if len(l) > 0 && l[len(l)-1] == '\r' {
			l = l[:len(l)-1]
		}
		return l, true
	}
	for len(b) > 0 {
		if b[0] != '*' {
			l, ok := line()
			if !ok {
				return false
			}
			if len(bytes.TrimSpace(l)) == 0 {
				continue // empty inline line: skipped by ReadCommand
			}
			return true
		}
		hdr, ok := line()
		if !ok {
			return false
		}
		n, err := parseLen(hdr[1:])
		if err != nil {
			return true // let ReadCommand report the protocol error
		}
		if n <= 0 {
			continue // "*0" / "*-1": skipped by ReadCommand
		}
		for i := 0; i < n; i++ {
			l, ok := line()
			if !ok {
				return false
			}
			if len(l) == 0 || l[0] != '$' {
				return true
			}
			sz, err := parseLen(l[1:])
			if err != nil || sz < 0 {
				return true
			}
			if len(b) < sz+2 {
				return false
			}
			b = b[sz+2:]
		}
		return true
	}
	return false
}

// Peek exposes bufio.Peek; used to detect disconnects of blocked clients.
func (r *Reader) Peek(n int) ([]byte, error) { return r.br.Peek(n) }

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErr("too big inline request")
		}
		if err == io.EOF && len(line) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	n := len(line) - 1
	if n > 0 && line[n-1] == '\r' {
		n--
	}
	return line[:n], nil
}

func parseLen(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, errors.New("empty length")
	}
	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
		if len(b) == 0 {
			return 0, errors.New("invalid length")
		}
	}
	n := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid length")
		}
		n = n*10 + int(c-'0')
		if n > MaxBulkLen {
			return 0, errors.New("length too large")
		}
	}
	if neg {
		return -n, nil
	}
	return n, nil
}

// ReadCommand reads one request and returns its arguments. Returned slices
// are freshly allocated and safe to retain.
func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		b, err := r.br.ReadByte()
		if err != nil {
			return nil, err
		}
		if b != '*' {
			r.br.UnreadByte()
			args, err := r.readInline()
			if err != nil {
				return nil, err
			}
			if len(args) == 0 {
				continue // empty line: ignore, like Redis
			}
			return args, nil
		}
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		n, err := parseLen(line)
		if err != nil || n > MaxMultiBulkLen {
			return nil, protoErr("invalid multibulk length")
		}
		if n <= 0 {
			continue
		}
		// All arguments share one backing buffer (2 allocations per command
		// instead of n+1). Each arg is capped with a full slice expression so
		// an append() on a stored value can never overwrite its neighbour.
		offs := make([]int, 0, 2*n)
		buf := make([]byte, 0, 64)
		for i := 0; i < n; i++ {
			b, err := r.br.ReadByte()
			if err != nil {
				return nil, unexpected(err)
			}
			if b != '$' {
				return nil, protoErr("expected '$', got '%c'", b)
			}
			line, err := r.readLine()
			if err != nil {
				return nil, unexpected(err)
			}
			l, err := parseLen(line)
			if err != nil || l < 0 {
				return nil, protoErr("invalid bulk length")
			}
			start := len(buf)
			buf = append(buf, make([]byte, l+2)...)
			if _, err := io.ReadFull(r.br, buf[start:]); err != nil {
				return nil, unexpected(err)
			}
			if buf[start+l] != '\r' || buf[start+l+1] != '\n' {
				return nil, protoErr("bulk string not terminated by CRLF")
			}
			buf = buf[:start+l]
			offs = append(offs, start, start+l)
		}
		args := make([][]byte, n)
		for i := range args {
			a, b := offs[2*i], offs[2*i+1]
			args[i] = buf[a:b:b]
		}
		return args, nil
	}
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// readInline splits a telnet-style line, honouring "double" and 'single'
// quotes with backslash escapes in double quotes.
func (r *Reader) readInline() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) > maxInlineLen {
		return nil, protoErr("too big inline request")
	}
	return SplitArgs(string(line))
}

// SplitArgs splits a command line into arguments (also used by the web
// console). It mirrors Redis' sdssplitargs.
func SplitArgs(line string) ([][]byte, error) {
	var args [][]byte
	i := 0
	for {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= len(line) {
			return args, nil
		}
		var cur []byte
		inDq, inSq := false, false
		for i < len(line) {
			c := line[i]
			switch {
			case inDq:
				if c == '\\' && i+1 < len(line) {
					i++
					switch line[i] {
					case 'n':
						cur = append(cur, '\n')
					case 'r':
						cur = append(cur, '\r')
					case 't':
						cur = append(cur, '\t')
					case 'x':
						if i+2 < len(line) {
							if v, err := strconv.ParseUint(line[i+1:i+3], 16, 8); err == nil {
								cur = append(cur, byte(v))
								i += 2
								break
							}
						}
						cur = append(cur, 'x')
					default:
						cur = append(cur, line[i])
					}
				} else if c == '"' {
					if i+1 < len(line) && line[i+1] != ' ' && line[i+1] != '\t' {
						return nil, protoErr("unbalanced quotes in request")
					}
					inDq = false
				} else {
					cur = append(cur, c)
				}
			case inSq:
				if c == '\\' && i+1 < len(line) && line[i+1] == '\'' {
					i++
					cur = append(cur, '\'')
				} else if c == '\'' {
					if i+1 < len(line) && line[i+1] != ' ' && line[i+1] != '\t' {
						return nil, protoErr("unbalanced quotes in request")
					}
					inSq = false
				} else {
					cur = append(cur, c)
				}
			default:
				if c == ' ' || c == '\t' {
					goto done
				} else if c == '"' {
					inDq = true
				} else if c == '\'' {
					inSq = true
				} else {
					cur = append(cur, c)
				}
			}
			i++
		}
		if inDq || inSq {
			return nil, protoErr("unbalanced quotes in request")
		}
	done:
		if cur == nil {
			cur = []byte{}
		}
		args = append(args, cur)
	}
}

// ReadValue decodes any RESP value (client side).
func (r *Reader) ReadValue() (Value, error) {
	t, err := r.br.ReadByte()
	if err != nil {
		return Value{}, err
	}
	line, err := r.readLine()
	if err != nil {
		return Value{}, unexpected(err)
	}
	switch t {
	case '+', '-':
		return Value{Type: t, Str: string(line)}, nil
	case ':':
		n, err := strconv.ParseInt(string(line), 10, 64)
		if err != nil {
			return Value{}, protoErr("invalid integer")
		}
		return Value{Type: t, Int: n}, nil
	case '$':
		l, err := parseLen(line)
		if err != nil {
			return Value{}, protoErr("invalid bulk length")
		}
		if l < 0 {
			return Value{Type: t, Null: true}, nil
		}
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return Value{}, unexpected(err)
		}
		return Value{Type: t, Str: string(buf[:l])}, nil
	case '*':
		n, err := parseLen(line)
		if err != nil {
			return Value{}, protoErr("invalid array length")
		}
		if n < 0 {
			return Value{Type: t, Null: true}, nil
		}
		arr := make([]Value, n)
		for i := range arr {
			if arr[i], err = r.ReadValue(); err != nil {
				return Value{}, unexpected(err)
			}
		}
		return Value{Type: t, Array: arr}, nil
	}
	return Value{}, protoErr("unknown type byte '%c'", t)
}

// Interface converts a Value to plain Go types (handy for JSON).
func (v Value) Interface() any {
	switch v.Type {
	case '+':
		return v.Str
	case '-':
		return map[string]string{"error": v.Str}
	case ':':
		return v.Int
	case '$':
		if v.Null {
			return nil
		}
		return v.Str
	case '*':
		if v.Null {
			return nil
		}
		out := make([]any, len(v.Array))
		for i, e := range v.Array {
			out[i] = e.Interface()
		}
		return out
	}
	return nil
}

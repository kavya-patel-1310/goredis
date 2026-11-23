package resp

import (
	"strconv"
)

// Writer appends RESP-encoded replies to an in-memory buffer. The server
// owns when and how that buffer reaches the socket, which lets it batch
// replies for pipelined requests into a single write(2).
type Writer struct {
	Buf []byte
}

func (w *Writer) Reset()   { w.Buf = w.Buf[:0] }
func (w *Writer) Len() int { return len(w.Buf) }

func (w *Writer) Simple(s string) {
	w.Buf = append(w.Buf, '+')
	w.Buf = append(w.Buf, s...)
	w.Buf = append(w.Buf, '\r', '\n')
}

func (w *Writer) OK() { w.Buf = append(w.Buf, "+OK\r\n"...) }

// Error writes an error reply. msg should start with an error code, e.g.
// "ERR syntax error" or "WRONGTYPE ...".
func (w *Writer) Error(msg string) {
	w.Buf = append(w.Buf, '-')
	w.Buf = append(w.Buf, msg...)
	w.Buf = append(w.Buf, '\r', '\n')
}

func (w *Writer) Int(n int64) {
	w.Buf = append(w.Buf, ':')
	w.Buf = strconv.AppendInt(w.Buf, n, 10)
	w.Buf = append(w.Buf, '\r', '\n')
}

func (w *Writer) Bulk(b []byte) {
	w.Buf = append(w.Buf, '$')
	w.Buf = strconv.AppendInt(w.Buf, int64(len(b)), 10)
	w.Buf = append(w.Buf, '\r', '\n')
	w.Buf = append(w.Buf, b...)
	w.Buf = append(w.Buf, '\r', '\n')
}

func (w *Writer) BulkString(s string) {
	w.Buf = append(w.Buf, '$')
	w.Buf = strconv.AppendInt(w.Buf, int64(len(s)), 10)
	w.Buf = append(w.Buf, '\r', '\n')
	w.Buf = append(w.Buf, s...)
	w.Buf = append(w.Buf, '\r', '\n')
}

func (w *Writer) BulkInt(n int64) { w.BulkString(strconv.FormatInt(n, 10)) }

func (w *Writer) BulkFloat(f float64) { w.BulkString(FormatFloat(f)) }

func (w *Writer) Null()      { w.Buf = append(w.Buf, "$-1\r\n"...) }
func (w *Writer) NullArray() { w.Buf = append(w.Buf, "*-1\r\n"...) }

func (w *Writer) Array(n int) {
	w.Buf = append(w.Buf, '*')
	w.Buf = strconv.AppendInt(w.Buf, int64(n), 10)
	w.Buf = append(w.Buf, '\r', '\n')
}

func (w *Writer) BulkStrings(ss []string) {
	w.Array(len(ss))
	for _, s := range ss {
		w.BulkString(s)
	}
}

// FormatFloat renders floats like Redis (shortest repr, inf/-inf).
func FormatFloat(f float64) string {
	switch {
	case f > 1.7976931348623157e308:
		return "inf"
	case f < -1.7976931348623157e308:
		return "-inf"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// AppendCommand encodes args as a RESP multibulk (used by the AOF and clients).
func AppendCommand(buf []byte, args ...[]byte) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, a := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(a)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, a...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

// AppendCommandStrings is AppendCommand for string arguments.
func AppendCommandStrings(buf []byte, args ...string) []byte {
	bs := make([][]byte, len(args))
	for i, a := range args {
		bs[i] = []byte(a)
	}
	return AppendCommand(buf, bs...)
}

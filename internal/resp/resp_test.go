package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadCommandMultibulk(t *testing.T) {
	r := NewReader(strings.NewReader("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n"))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"SET", "key", "hello"}
	if len(args) != len(want) {
		t.Fatalf("got %d args", len(args))
	}
	for i := range want {
		if string(args[i]) != want[i] {
			t.Errorf("arg %d = %q want %q", i, args[i], want[i])
		}
	}
}

func TestReadCommandBinarySafe(t *testing.T) {
	payload := "a\r\nb\x00c"
	var buf []byte
	buf = AppendCommandStrings(buf, "SET", "k", payload)
	args, err := NewReader(bytes.NewReader(buf)).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if string(args[2]) != payload {
		t.Fatalf("got %q", args[2])
	}
}

func TestReadCommandInline(t *testing.T) {
	r := NewReader(strings.NewReader("\r\nSET  key \"hello world\\n\" 'it\\'s'\r\nPING\n"))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, a := range args {
		got = append(got, string(a))
	}
	want := []string{"SET", "key", "hello world\n", "it's"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %q", got)
	}
	args, err = r.ReadCommand()
	if err != nil || string(args[0]) != "PING" {
		t.Fatalf("second inline: %v %q", err, args)
	}
}

func TestReadCommandPipelined(t *testing.T) {
	var buf []byte
	for i := 0; i < 100; i++ {
		buf = AppendCommandStrings(buf, "INCR", "counter")
	}
	r := NewReader(bytes.NewReader(buf))
	for i := 0; i < 100; i++ {
		if _, err := r.ReadCommand(); err != nil {
			t.Fatalf("cmd %d: %v", i, err)
		}
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestReadCommandErrors(t *testing.T) {
	cases := []string{
		"*2\r\n$3\r\nGET\r\n:1\r\n", // wrong type inside multibulk
		"*1\r\n$-5\r\n",             // negative bulk len
		"*abc\r\n",                  // bad multibulk len
		"*1\r\n$3\r\nGETXX",         // missing CRLF terminator
		"SET \"unterminated\r\n",    // bad quotes
		"*99999999999\r\n",          // too many args
	}
	for _, c := range cases {
		_, err := NewReader(strings.NewReader(c)).ReadCommand()
		if err == nil {
			t.Errorf("expected error for %q", c)
		}
	}
	_, err := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n")).ReadCommand()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("truncated: want ErrUnexpectedEOF, got %v", err)
	}
}

func TestHasCompleteCommand(t *testing.T) {
	cases := map[string]bool{
		"":                           false,
		"*1\r\n$4\r\nPING\r\n":       true,
		"*1\r\n$4\r\nPI":             false,
		"*2\r\n$3\r\nGET\r\n":        false,
		"PING\r\n":                   true,
		"PIN":                        false,
		"*0\r\n":                     false, // empty command: nothing to execute
		"\r\n\r\n":                   false,
		"*0\r\n*1\r\n$4\r\nPING\r\n": true,
	}
	for in, want := range cases {
		r := NewReader(strings.NewReader(in))
		r.Peek(1) // fill the buffer
		if got := r.HasCompleteCommand(); got != want {
			t.Errorf("HasCompleteCommand(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestWriterAndReadValue(t *testing.T) {
	var w Writer
	w.Array(6)
	w.Simple("OK")
	w.Error("ERR boom")
	w.Int(-42)
	w.BulkString("hello")
	w.Null()
	w.Array(1)
	w.BulkFloat(3.5)

	v, err := NewReader(bytes.NewReader(w.Buf)).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if v.Type != '*' || len(v.Array) != 6 {
		t.Fatalf("bad array %+v", v)
	}
	a := v.Array
	if a[0].Str != "OK" || a[1].Str != "ERR boom" || a[2].Int != -42 || a[3].Str != "hello" || !a[4].Null || a[5].Array[0].Str != "3.5" {
		t.Fatalf("unexpected decode: %+v", a)
	}
}

func TestFormatFloat(t *testing.T) {
	for in, want := range map[float64]string{1: "1", 1.5: "1.5", -0.25: "-0.25", 1e20: "100000000000000000000"} {
		if got := FormatFloat(in); got != want {
			t.Errorf("FormatFloat(%v)=%q want %q", in, got, want)
		}
	}
}

// FuzzReadCommand ensures the parser never panics on arbitrary input.
// Run with: go test -fuzz=FuzzReadCommand ./internal/resp
func FuzzReadCommand(f *testing.F) {
	f.Add([]byte("*1\r\n$4\r\nPING\r\n"))
	f.Add([]byte("SET a \"b c\"\r\n"))
	f.Add([]byte("*2\r\n$3\r\nGET\r\n$-1\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for i := 0; i < 16; i++ {
			if _, err := r.ReadCommand(); err != nil {
				return
			}
		}
	})
}

func BenchmarkReadCommand(b *testing.B) {
	cmd := AppendCommandStrings(nil, "SET", "key:000000012345", "some-value-of-moderate-length")
	data := bytes.Repeat(cmd, 1000)
	b.SetBytes(int64(len(cmd)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; {
		r := NewReader(bytes.NewReader(data))
		for j := 0; j < 1000 && i < b.N; j++ {
			r.ReadCommand()
			i++
		}
	}
}

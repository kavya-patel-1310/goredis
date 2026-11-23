package glob

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		p, s string
		want bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"user:*", "user:42", true},
		{"user:*", "session:42", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[a-c]llo", "hdllo", false},
		{"h\\*llo", "h*llo", true},
		{"h\\*llo", "hello", false},
		{"*:*:*", "a:b:c", true},
		{"*:*:*", "a:b", false},
		{"a*b*c", "aXXbYYc", true},
		{"a*b*c", "aXXbYY", false},
		{"news.*", "news.tech", true},
	}
	for _, c := range cases {
		if got := Match(c.p, c.s); got != c.want {
			t.Errorf("Match(%q,%q)=%v want %v", c.p, c.s, got, c.want)
		}
	}
}

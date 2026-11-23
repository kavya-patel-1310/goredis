// Package glob implements Redis-style glob matching used by KEYS, SCAN
// MATCH and PSUBSCRIBE: *, ?, [abc], [^abc], [a-z] and \ escapes.
package glob

// Match reports whether s matches pattern.
func Match(pattern, s string) bool { return match(pattern, s, 0) }

func match(p, s string, depth int) bool {
	if depth > 1000 { // guard against pathological patterns
		return false
	}
	for len(p) > 0 {
		switch p[0] {
		case '*':
			for len(p) > 1 && p[1] == '*' {
				p = p[1:]
			}
			if len(p) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if match(p[1:], s[i:], depth+1) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			s = s[1:]
			p = p[1:]
		case '[':
			if len(s) == 0 {
				return false
			}
			p = p[1:]
			not := len(p) > 0 && p[0] == '^'
			if not {
				p = p[1:]
			}
			matched := false
			for len(p) > 0 && p[0] != ']' {
				switch {
				case p[0] == '\\' && len(p) >= 2:
					if p[1] == s[0] {
						matched = true
					}
					p = p[2:]
				case len(p) >= 3 && p[1] == '-':
					lo, hi := p[0], p[2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if s[0] >= lo && s[0] <= hi {
						matched = true
					}
					p = p[3:]
				default:
					if p[0] == s[0] {
						matched = true
					}
					p = p[1:]
				}
			}
			if len(p) > 0 {
				p = p[1:] // skip ']'
			}
			if matched == not {
				return false
			}
			s = s[1:]
		case '\\':
			if len(p) >= 2 {
				p = p[1:]
			}
			if len(s) == 0 || p[0] != s[0] {
				return false
			}
			s, p = s[1:], p[1:]
		default:
			if len(s) == 0 || p[0] != s[0] {
				return false
			}
			s, p = s[1:], p[1:]
		}
	}
	return len(s) == 0
}

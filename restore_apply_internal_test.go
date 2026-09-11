package litestream

import "testing"

func TestPageSet(t *testing.T) {
	s := newPageSet(130)
	if got := s.firstMissing(130, 0); got != 0 {
		t.Fatalf("full set: firstMissing = %d, want 0", got)
	}

	// Truncating on a word boundary edge keeps exactly the pages at or below n.
	for _, n := range []uint32{63, 64, 127, 128, 129} {
		c := newPageSet(130)
		c.truncate(n)
		if !c.has(n) || c.has(n+1) {
			t.Fatalf("truncate(%d): has(%d)=%v has(%d)=%v", n, n, c.has(n), n+1, c.has(n+1))
		}
		if got := c.firstMissing(n+2, 0); got != n+1 {
			t.Fatalf("truncate(%d): firstMissing = %d, want %d", n, got, n+1)
		}
	}

	// A page past the end grows the set; a skipped page is never missing.
	s.add(200)
	if got := s.firstMissing(200, 0); got != 131 {
		t.Fatalf("after add(200): firstMissing = %d, want 131", got)
	}
	g := newPageSet(10)
	g.add(12)
	if got := g.firstMissing(12, 11); got != 0 {
		t.Fatalf("with skip: firstMissing = %d, want 0", got)
	}
}

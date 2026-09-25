package version

import "testing"

func testPolicy() Policy {
	return Policy{
		MinSupported: MustParse("1.1.0"),
		Known: []Version{
			MustParse("1.0.0"), MustParse("1.1.0"),
			MustParse("1.2.0"), MustParse("2.0.0"),
		},
	}
}

// MustParse 仅测试辅助。

func TestParseAndCompare(t *testing.T) {
	v, err := Parse("1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "1.2.3" {
		t.Fatalf("got %s", v)
	}
	if c := v.Compare(MustParse("1.3.0")); c != -1 {
		t.Fatalf("want -1 got %d", c)
	}
	if _, err := Parse("nope"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestPolicyAccept(t *testing.T) {
	p := testPolicy()
	cases := []struct {
		in     string
		reject RejectReason
		ok     bool
	}{
		{"1.2.0", "", true},
		{"2.0.0", "", true},
		{"1.0.0", RejectTooOld, false}, // 已知但旧于下限
		{"3.1.0", RejectUnknownVersion, false},
		{"9.9.9", RejectUnknownVersion, false},
	}
	for _, c := range cases {
		err := p.Accept(MustParse(c.in))
		if c.ok {
			if err != nil {
				t.Fatalf("%s: unexpected reject: %v", c.in, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("%s: expected reject", c.in)
		}
		ne := err.(*NegotiateError)
		if ne.Reason != c.reject {
			t.Fatalf("%s: want reason %s got %s", c.in, c.reject, ne.Reason)
		}
	}
}

func TestSemanticCompatible(t *testing.T) {
	off := Capability{Name: "pay", Semantic: 2}
	if !SemanticCompatible(RequiredCapability{Name: "pay", MinSemantic: 1}, off) {
		t.Fatal("offered semantic 2 should satisfy min 1")
	}
	if SemanticCompatible(RequiredCapability{Name: "pay", MinSemantic: 3}, off) {
		t.Fatal("offered 2 must not satisfy min 3")
	}
	if !SemanticCompatible(RequiredCapability{Name: "pay", MinSemantic: 2, ExactSemantic: true}, off) {
		t.Fatal("exact match should pass")
	}
	if SemanticCompatible(RequiredCapability{Name: "pay", MinSemantic: 1, ExactSemantic: true}, off) {
		t.Fatal("exact semantic requirement must reject different generation")
	}
	if SemanticCompatible(RequiredCapability{Name: "refund", MinSemantic: 1}, off) {
		t.Fatal("different capability name must not match")
	}
}

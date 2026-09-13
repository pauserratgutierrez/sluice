package hunt

import "testing"

func TestSearchFindsBoundary(t *testing.T) {
	limit := 750
	var seen []int
	max, tried, err := Search(Config{Start: 100, Cap: 4000, Granularity: 50}, func(n int) (bool, error) {
		seen = append(seen, n)
		return n <= limit, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if max < 700 || max > 750 {
		t.Fatalf("max=%d tried=%v seen=%v", max, tried, seen)
	}
	if len(tried) < 4 {
		t.Fatalf("expected a ladder, got %v", tried)
	}
	if tried[0] != 100 || tried[1] != 200 {
		t.Fatalf("ladder start %v", tried)
	}
}

func TestSearchCapPasses(t *testing.T) {
	max, tried, err := Search(Config{Start: 10, Cap: 40, Granularity: 5}, func(n int) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if max != 40 {
		t.Fatalf("max=%d tried=%v", max, tried)
	}
}

func TestSearchStartFails(t *testing.T) {
	max, _, err := Search(Config{Start: 50, Cap: 200, Granularity: 10}, func(int) (bool, error) {
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if max != 0 {
		t.Fatalf("max=%d", max)
	}
}

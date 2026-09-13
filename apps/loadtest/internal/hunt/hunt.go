package hunt

import "fmt"

type Config struct {
	Start       int
	Cap         int
	Granularity int
}

// Search doubles n from Start until a probe fails or Cap is reached, then
// binary-searches the last value that still passes. probe must be deterministic
// enough that a fail at N implies fail at >N.
func Search(cfg Config, probe func(n int) (pass bool, err error)) (max int, tried []int, err error) {
	if cfg.Start < 1 {
		cfg.Start = 1
	}
	if cfg.Cap < cfg.Start {
		cfg.Cap = cfg.Start
	}
	if cfg.Granularity < 1 {
		cfg.Granularity = 1
	}

	lastPass := 0
	firstFail := 0
	n := cfg.Start
	for {
		if n > cfg.Cap {
			n = cfg.Cap
		}
		tried = append(tried, n)
		pass, err := probe(n)
		if err != nil {
			return lastPass, tried, err
		}
		if pass {
			lastPass = n
			if n >= cfg.Cap {
				return lastPass, tried, nil
			}
			next := n * 2
			if next <= n {
				return lastPass, tried, fmt.Errorf("hunt: overflow at %d", n)
			}
			n = next
			continue
		}
		firstFail = n
		break
	}
	if lastPass == 0 {
		return 0, tried, nil
	}
	lo, hi := lastPass, firstFail
	for hi-lo > cfg.Granularity {
		mid := lo + (hi-lo)/2
		mid = roundDown(mid, cfg.Granularity)
		if mid <= lo {
			mid = lo + cfg.Granularity
		}
		if mid >= hi {
			break
		}
		tried = append(tried, mid)
		pass, err := probe(mid)
		if err != nil {
			return lastPass, tried, err
		}
		if pass {
			lo = mid
			lastPass = mid
		} else {
			hi = mid
		}
	}
	return lastPass, tried, nil
}

func roundDown(n, grain int) int {
	if grain <= 1 {
		return n
	}
	return n - (n % grain)
}

package ws

import (
	"math/rand/v2"
	"time"
)

const (
	codeNormal   = 1000
	codeAbnormal = 1006
	codeBudget   = 1013
	codeLifetime = 4408
	codeTooSlow  = 4429
)

type plan struct {
	retry bool
	scale int
}

func planFor(code int) plan {
	switch code {
	case codeBudget:
		return plan{retry: true, scale: 4}
	case codeLifetime:
		return plan{retry: true, scale: 0}
	case codeTooSlow:
		return plan{retry: true, scale: 1}
	case codeNormal, 1002, 1003, 1007, 1008, 4401, 4403:
		return plan{}
	}
	if code >= 3000 {
		return plan{}
	}
	return plan{retry: true, scale: 1}
}

func delayFor(p plan, attempt int, minWait, maxWait time.Duration, random func() float64) time.Duration {
	if p.scale == 0 {
		return time.Duration(random() * float64(minWait))
	}
	ceiling := maxWait
	if attempt < 30 {
		if c := minWait * time.Duration(p.scale) << attempt; c > 0 && c < maxWait {
			ceiling = c
		}
	}
	return time.Duration(random() * float64(ceiling))
}

func jitter() float64 {
	return rand.Float64() //nolint:gosec // backoff jitter needs no secrecy
}

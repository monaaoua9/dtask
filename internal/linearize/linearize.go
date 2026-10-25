// Package linearize is a small Wing & Gong style linearizability checker.
//
// Feed it the operations one object saw (call time, return time, input,
// output) and a sequential model; it answers whether some total order exists
// that respects real time and the model. Histories are checked per object, so
// keep them short: the search is exponential in the worst case and the visited
// set is keyed by a 64-bit mask.
package linearize

import (
	"fmt"
	"math"
	"strings"
)

type Op struct {
	Call, Ret int64 // monotonic ns; Ret is ignored when Indef
	// Indef marks a call whose outcome the client never learned (timeout,
	// leadership change). It may have taken effect at any point after Call, or
	// never. Step is asked whether it could apply and must ignore Out.
	Indef bool
	In    any
	Out   any
}

type Model struct {
	Init func() any
	// Step returns the next state if op is legal in state. States must be
	// comparable; they are used as map keys.
	Step func(state any, op Op) (any, bool)
}

const MaxOps = 64

type memoKey struct {
	mask  uint64
	state any
}

func Check(ops []Op, m Model) bool {
	if len(ops) > MaxOps {
		panic(fmt.Sprintf("linearize: %d ops, limit is %d", len(ops), MaxOps))
	}
	ret := make([]int64, len(ops))
	var all, definite uint64
	for i, op := range ops {
		ret[i] = op.Ret
		if op.Indef {
			ret[i] = math.MaxInt64
		} else {
			definite |= 1 << i
		}
		all |= 1 << i
	}

	seen := map[memoKey]bool{}
	var walk func(rem uint64, st any) bool
	walk = func(rem uint64, st any) bool {
		if rem&definite == 0 {
			return true
		}
		k := memoKey{rem, st}
		if seen[k] {
			return false
		}

		// an op can go next only if nothing still pending finished before it began
		minRet := int64(math.MaxInt64)
		for i := range ops {
			if rem&(1<<i) != 0 && ret[i] < minRet {
				minRet = ret[i]
			}
		}
		for i, op := range ops {
			if rem&(1<<i) == 0 || op.Call > minRet {
				continue
			}
			if next, ok := m.Step(st, op); ok && walk(rem&^(1<<i), next) {
				return true
			}
		}
		seen[k] = true
		return false
	}
	return walk(all, m.Init())
}

// Describe renders a history for failure messages, ordered as given.
func Describe(ops []Op) string {
	var b strings.Builder
	for _, op := range ops {
		ret := fmt.Sprint(op.Ret)
		if op.Indef {
			ret = "?"
		}
		fmt.Fprintf(&b, "  [%d .. %s] %v -> %v\n", op.Call, ret, op.In, op.Out)
	}
	return b.String()
}

package linearize

import "testing"

type rw struct {
	write bool
	v     int
}

var register = Model{
	Init: func() any { return 0 },
	Step: func(s any, op Op) (any, bool) {
		in := op.In.(rw)
		if in.write {
			return in.v, true
		}
		if op.Indef {
			return s, true
		}
		return s, op.Out.(int) == s.(int)
	},
}

func w(call, ret int64, v int) Op { return Op{Call: call, Ret: ret, In: rw{true, v}} }
func r(call, ret int64, v int) Op { return Op{Call: call, Ret: ret, In: rw{false, 0}, Out: v} }

func TestSequentialHistory(t *testing.T) {
	if !Check([]Op{w(0, 1, 5), r(2, 3, 5), w(4, 5, 7), r(6, 7, 7)}, register) {
		t.Fatal("plain sequential history rejected")
	}
}

func TestStaleReadIsCaught(t *testing.T) {
	// the write finished before the read began, yet the read saw the old value
	if Check([]Op{w(0, 1, 5), r(2, 3, 0)}, register) {
		t.Fatal("stale read accepted")
	}
}

func TestOverlapAllowsEitherOrder(t *testing.T) {
	// read overlaps the write, so both 0 and 5 are fine
	for _, seen := range []int{0, 5} {
		if !Check([]Op{w(0, 10, 5), r(2, 3, seen)}, register) {
			t.Fatalf("read of %d during overlapping write rejected", seen)
		}
	}
}

func TestReadsCannotGoBackwards(t *testing.T) {
	// two sequential reads: new value then old value, with a single write
	h := []Op{w(0, 20, 5), r(1, 2, 5), r(3, 4, 0)}
	if Check(h, register) {
		t.Fatal("time-travelling read accepted")
	}
}

func TestIndefiniteWriteMayOrMayNotApply(t *testing.T) {
	timedOut := Op{Call: 0, Indef: true, In: rw{true, 9}}
	if !Check([]Op{timedOut, r(5, 6, 9)}, register) {
		t.Fatal("write that did apply was rejected")
	}
	if !Check([]Op{timedOut, r(5, 6, 0)}, register) {
		t.Fatal("write that never applied was rejected")
	}
	// but it can't un-apply: once seen, it stays
	if Check([]Op{timedOut, r(5, 6, 9), r(7, 8, 0)}, register) {
		t.Fatal("value vanished after being observed")
	}
}

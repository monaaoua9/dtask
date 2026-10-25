package fsm

import (
	"container/heap"
	"math/bits"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
)

// heap entries are validated lazily when popped: a task that was cancelled,
// re-queued or deleted since the entry was pushed has a different version
type qitem struct {
	t     *pb.Task
	ver   uint64
	prio  int32 // ordering keys are copied so mutating the task can't corrupt a heap
	seq   uint64
	avail int64
}

type readyHeap []qitem

func (h readyHeap) Len() int { return len(h) }
func (h readyHeap) Less(i, j int) bool {
	if h[i].prio != h[j].prio {
		return h[i].prio > h[j].prio
	}
	return h[i].seq < h[j].seq
}
func (h readyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *readyHeap) Push(x any)   { *h = append(*h, x.(qitem)) }
func (h *readyHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type delayedHeap []qitem

func (h delayedHeap) Len() int { return len(h) }
func (h delayedHeap) Less(i, j int) bool {
	if h[i].avail != h[j].avail {
		return h[i].avail < h[j].avail
	}
	return h[i].seq < h[j].seq
}
func (h delayedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *delayedHeap) Push(x any)   { *h = append(*h, x.(qitem)) }
func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type queue struct {
	cfg        *pb.QueueConfig
	tokens     int64 // milli-tokens
	lastRefill int64
	ready      readyHeap
	delayed    delayedHeap
	inFlight   int
}

func newQueue(name string) *queue {
	return &queue{cfg: &pb.QueueConfig{Name: name}}
}

func (q *queue) limited() bool { return q.cfg.RatePerSec > 0 }

func (q *queue) capacity() int64 {
	b := int64(q.cfg.Burst)
	if b < 1 {
		b = 1
	}
	return b * 1000
}

// rateMilli is the refill speed in milli-tokens per second. Integer maths
// only: every replica has to land on the same number.
func (q *queue) rateMilli() int64 { return int64(q.cfg.RatePerSec * 1000) }

func (q *queue) configure(cfg *pb.QueueConfig, now int64) {
	q.cfg = cfg
	q.tokens = q.capacity()
	q.lastRefill = now
}

func (q *queue) refill(now int64) {
	rate := q.rateMilli()
	if !q.limited() || rate <= 0 {
		return
	}
	capacity := q.capacity()
	if q.tokens >= capacity {
		q.tokens, q.lastRefill = capacity, now
		return
	}
	el := now - q.lastRefill
	if el <= 0 {
		return
	}
	const maxElapsed = int64(3600e9)
	if el > maxElapsed {
		el = maxElapsed
	}
	hi, lo := bits.Mul64(uint64(el), uint64(rate))
	add64, _ := bits.Div64(hi, lo, 1e9)
	add := int64(add64)
	if add == 0 {
		return // keep lastRefill so sub-milli-token slices accumulate
	}
	q.tokens += add
	if q.tokens >= capacity {
		q.tokens, q.lastRefill = capacity, now
		return
	}
	q.lastRefill += add * 1e9 / rate
}

func (f *FSM) enqueue(t *pb.Task, now int64) {
	q := f.queue(t.Queue)
	f.ver++
	f.qver[t.Id] = f.ver
	it := qitem{t: t, ver: f.ver, prio: t.Priority, seq: t.Seq, avail: t.AvailableAtNs}
	if t.AvailableAtNs > now {
		heap.Push(&q.delayed, it)
	} else {
		heap.Push(&q.ready, it)
	}
}

func (f *FSM) valid(it qitem) bool {
	return f.qver[it.t.Id] == it.ver && it.t.State == pb.TaskState_TASK_STATE_PENDING
}

// promote moves delayed tasks whose time has come into the ready heap.
func (f *FSM) promote(q *queue, now int64) {
	for q.delayed.Len() > 0 {
		top := q.delayed[0]
		if !f.valid(top) {
			heap.Pop(&q.delayed)
			continue
		}
		if top.avail > now {
			return
		}
		heap.Pop(&q.delayed)
		heap.Push(&q.ready, top)
	}
}

func (f *FSM) popReady(q *queue) *pb.Task {
	for q.ready.Len() > 0 {
		it := heap.Pop(&q.ready).(qitem)
		if f.valid(it) {
			delete(f.qver, it.t.Id)
			return it.t
		}
	}
	return nil
}

func (f *FSM) peekReady(q *queue) *pb.Task {
	for q.ready.Len() > 0 {
		if it := q.ready[0]; f.valid(it) {
			return it.t
		}
		heap.Pop(&q.ready)
	}
	return nil
}

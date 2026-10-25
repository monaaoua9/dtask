package fsm

import (
	"container/heap"
	"sort"

	"google.golang.org/protobuf/proto"

	pb "github.com/monaaoua9/dtask/api/gen/dtask/v1"
)

func (f *FSM) Get(id string) (*pb.Task, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, ok := f.tasks[id]
	if !ok {
		return nil, false
	}
	return proto.Clone(t).(*pb.Task), true
}

// List returns tasks in submission order, starting after afterSeq. state and
// queue are optional filters. more says whether another page exists.
func (f *FSM) List(queue string, state pb.TaskState, limit int, afterSeq uint64) (page []*pb.Task, more bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var match []*pb.Task
	for _, t := range f.tasks {
		if t.Seq <= afterSeq {
			continue
		}
		if queue != "" && t.Queue != queue {
			continue
		}
		if state != pb.TaskState_TASK_STATE_UNSPECIFIED && t.State != state {
			continue
		}
		match = append(match, t)
	}
	sort.Slice(match, func(i, j int) bool { return match[i].Seq < match[j].Seq })
	if len(match) > limit {
		match, more = match[:limit], true
	}
	for _, t := range match {
		page = append(page, proto.Clone(t).(*pb.Task))
	}
	return page, more
}

type QueueStats struct {
	Name      string
	Pending   uint64
	Delayed   uint64
	Leased    uint64
	Succeeded uint64
	Dead      uint64
	Canceled  uint64
	Config    *pb.QueueConfig
}

func (f *FSM) Stats(now int64) []QueueStats {
	f.mu.RLock()
	defer f.mu.RUnlock()
	by := map[string]*QueueStats{}
	for name, q := range f.queues {
		by[name] = &QueueStats{Name: name, Config: proto.Clone(q.cfg).(*pb.QueueConfig)}
	}
	for _, t := range f.tasks {
		s := by[t.Queue]
		if s == nil {
			s = &QueueStats{Name: t.Queue}
			by[t.Queue] = s
		}
		switch t.State {
		case pb.TaskState_TASK_STATE_PENDING:
			if t.AvailableAtNs > now {
				s.Delayed++
			} else {
				s.Pending++
			}
		case pb.TaskState_TASK_STATE_LEASED:
			s.Leased++
		case pb.TaskState_TASK_STATE_SUCCEEDED:
			s.Succeeded++
		case pb.TaskState_TASK_STATE_DEAD:
			s.Dead++
		case pb.TaskState_TASK_STATE_CANCELED:
			s.Canceled++
		}
	}
	out := make([]QueueStats, 0, len(by))
	for _, s := range by {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// NextLeaseExpiry is the earliest lease deadline, 0 when nothing is leased.
func (f *FSM) NextLeaseExpiry() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var min int64
	for _, t := range f.leased {
		if min == 0 || t.LeaseExpiresAtNs < min {
			min = t.LeaseExpiresAtNs
		}
	}
	return min
}

// HasReady reports whether a Lease for these queues could hand out a task
// right now. It lets the leader skip appending empty polls to the log.
// It only drops dead heap entries, so replicated state is untouched.
func (f *FSM) HasReady(queues []string, now int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, name := range queues {
		q := f.queues[name]
		if q == nil {
			continue
		}
		if q.cfg.MaxInFlight > 0 && q.inFlight >= int(q.cfg.MaxInFlight) {
			continue
		}
		if q.limited() {
			probe := *q
			probe.refill(now)
			if probe.tokens < 1000 {
				continue
			}
		}
		if f.peekReady(q) != nil {
			return true
		}
		for q.delayed.Len() > 0 && !f.valid(q.delayed[0]) {
			heap.Pop(&q.delayed)
		}
		if q.delayed.Len() > 0 && q.delayed[0].avail <= now {
			return true
		}
	}
	return false
}

func (f *FSM) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.tasks)
}

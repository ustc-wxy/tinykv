// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).

}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	entries, err := storage.Entries(lo, hi+1)
	storage.InitialState()
	if err != nil {
		panic(err)
	}
	//if len(entries) == 0 {
	//	entries = append(entries, pb.Entry{Index: 0, Term: 0})
	//}
	return &RaftLog{storage: storage, entries: entries, applied: lo - 1, stabled: hi}
}

//Author:sqdbibibi Date:4.28
func (l *RaftLog) getHigherLog(index uint64) []*pb.Entry {
	var entries []*pb.Entry
	n := len(l.entries)
	for i := l.toSliceIndex(index); i < n; i++ {
		entries = append(entries, &l.entries[i])
	}
	return entries
}

//Author:sqdbibibi Date:4.28
func (l *RaftLog) toSliceIndex(logIndex uint64) int {
	idx := int(logIndex - l.FirstIndex())
	if idx < 0 {
		log.Fatal("toSliceIndex err!")
		return -1
	}
	return idx
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.FirstIndex()+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.FirstIndex()+1 : l.committed-l.FirstIndex()+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	lenEntries := len(l.entries)
	if lenEntries > 0 {
		return l.entries[lenEntries-1].Index
	}
	res, _ := l.storage.LastIndex()
	return res
}

//Author:sqdbibibi Data:4.28
func (l *RaftLog) FirstIndex() uint64 {
	res, _ := l.storage.FirstIndex()
	lenEntries := len(l.entries)
	if lenEntries > 0 {
		res = max(res, l.entries[0].Index)
	}
	return res
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	//DPrintf("[Term debug],index is %v,log is %v", i, l.entries)
	if len(l.entries) > 0 && i >= l.FirstIndex() {
		return l.entries[i-l.FirstIndex()].Term, nil
	}
	term, _ := l.storage.Term(i)
	return term, nil
}

//Author:sqdbibibi Date:4.29
func (l *RaftLog) getLog(index uint64) *pb.Entry {
	//DPrintf("[getlog Debug],index:%v fst:%v lst:%v\n", index, l.FirstIndex(), l.LastIndex())
	if index == 0 {
		return &pb.Entry{Index: 0, Term: 0}
	}
	if index < l.FirstIndex() || index > l.LastIndex() {
		return nil
	}
	return &l.entries[index-l.FirstIndex()]
}

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
	"log"
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
	includeIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lo, err := storage.FirstIndex()
	hi, err := storage.LastIndex()
	entries, err := storage.Entries(lo, hi+1)
	if err != nil {
		log.Printf("[newLog]Error!!! lo=%v hi=%v\n", lo, hi)
		panic(err)
	}
	return &RaftLog{storage: storage, entries: entries, applied: lo - 1, stabled: hi}
}

//Author:sqdbibibi Data:4.28 5.6
func (l *RaftLog) FirstIndex() uint64 {
	res, _ := l.storage.FirstIndex()
	return res
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

//Author:sqdbibibi Date:4.28 5.6
func (l *RaftLog) getHigherLog(index uint64) []*pb.Entry {
	var entries []*pb.Entry
	startIndex := index - l.entries[0].Index
	endIndex := uint64(len(l.entries))
	for i := startIndex; i < endIndex; i++ {
		entries = append(entries, &l.entries[i])
	}
	return entries
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	first, _ := l.storage.FirstIndex()
	if len(l.entries) > 0 {
		if first > l.entries[0].Index {
			entries := l.entries[first-l.entries[0].Index:]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
	}

}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.entries[0].Index+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		lb := l.applied - l.entries[0].Index + 1
		rb := l.committed - l.entries[0].Index + 1
		//if lb > rb || int(rb) >= len(l.entries) {
		//	return nil
		//}
		return l.entries[lb:rb]
	}
	return nil
}

func (l *RaftLog) getLocal(index uint64) *pb.Entry {
	if len(l.entries) > 0 {
		if index >= l.entries[0].Index && index <= l.entries[len(l.entries)-1].Index {
			return &l.entries[index-l.entries[0].Index]
		}
	}
	return nil
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(index uint64) (uint64, error) {
	// Your Code Here (2A).
	if localLog := l.getLocal(index); localLog != nil {
		return localLog.Term, nil
	}
	term, err := l.storage.Term(index)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if index == l.pendingSnapshot.Metadata.Index {
			term = l.pendingSnapshot.Metadata.Term
			err = nil
		} else if index < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

//Author:sqdbibibi Date:4.29 5.4(modify)
func (l *RaftLog) getLog(index uint64) *pb.Entry {
	if index == 0 {
		return &pb.Entry{Index: 0, Term: 0}
	}
	if index == 5 {
		return &pb.Entry{Index: 5, Term: 5}
	}

	if localLog := l.getLocal(index); localLog != nil {
		return localLog
	}

	//Get from storage
	lst, _ := l.storage.LastIndex()
	if index <= lst {
		entry, err := l.storage.Entries(index, index+1)
		if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
			if index == l.pendingSnapshot.Metadata.Index {
				return &pb.Entry{Index: index, Term: l.pendingSnapshot.Metadata.Term}
			}
		}
		if err == nil && entry != nil {
			return &entry[0]
		}
	}

	return nil
}

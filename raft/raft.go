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
	"errors"
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"math/rand"
	"sort"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var stateArray = [3]string{
	"Follower",
	"Candidate",
	"Leader",
}

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout       int
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {

	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, i := range c.peers {
		if i == r.id {
			r.Prs[i] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			r.Prs[i] = &Progress{Next: lastIndex + 1}
		}
	}
	r.becomeFollower(0, None)
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	DPrintf("%s is loading..., log content: %v\n", r, r.RaftLog.entries)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	DPrintf("%s is send append to %v,log is %v\n", r, to, r.RaftLog.entries)
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		if err == ErrCompacted {
			//r.sendSnapshot(to)
			return false
		}
		panic(err)
	}
	entries := r.RaftLog.getHigherLog(prevLogIndex + 1)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entries,
	}
	DPrintf("Append msg is finish, from %v to %v,index %v,logterm %v, commited %v,entries %v",
		msg.From, msg.To, msg.Index, msg.LogTerm, msg.Commit, msg.Entries)
	r.msgs = append(r.msgs, msg)
	return true
}

//Author:sqdbibibi Date:4.28
func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64, term uint64) {
	DPrintf("%s is send Append Response to Leader\n", r)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    term,
		Reject:  reject,
		Index:   index,
		LogTerm: term,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	DPrintf("%s is send hb to %v\n", r, to)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

//Author:sqdbibibi Date:4.28
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	DPrintf("%s will send Heartbeat Response to leader.\n", r)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}
func (r *Raft) sendRequestVote(to, lastLogIndex, lastLogTerm uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   lastLogIndex,
		LogTerm: lastLogTerm,
	}
	r.msgs = append(r.msgs, msg)
}
func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}
func (r *Raft) tickHeartBeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.tickElection()
	case StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartBeat()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Vote = None
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Vote = r.id
	r.Term++
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex()
	// NOTE: Leader should propose a noop entry on its term
	noopEntry := pb.Entry{Term: r.Term, Index: lastIndex + 1}
	r.RaftLog.entries = append(r.RaftLog.entries, noopEntry)
	//lastIndex++
	for i := range r.Prs {
		if i == r.id {
			r.Prs[i].Next = lastIndex + 1
			r.Prs[i].Match = lastIndex
		} else {
			r.Prs[i].Next = lastIndex + 1
			r.Prs[i].Match = 0
		}
	}
	DPrintf("%s become a leader,log is %v.\n", r, r.RaftLog.entries)
	r.heartbeatElapsed = 0
	r.broadcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}
func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.doElection()
	}
	return nil
}
func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}
func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.broadcastHeartbeat()
	case pb.MessageType_MsgPropose:
		if r.leadTransferee == None {
			r.start(m.Entries)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)
	case pb.MessageType_MsgTransferLeader:
		//r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
	}
	return nil
}

//Author:sqdbibibi Date:4.29
func (r *Raft) start(ents []*pb.Entry) {
	DPrintf("%s receive new commands,size is %v,lastIndex is %v.\n", r, len(ents), r.RaftLog.LastIndex())
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range ents {
		entry.Index = lastIndex + uint64(i+1)
		entry.Term = r.Term

		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	r.broadcastAppend()
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint64
	VoteGranted bool
}

func (r *Raft) doElection() {
	r.becomeCandidate()
	r.heartbeatElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	DPrintf("%s do an election.\n", r)
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.sendRequestVote(i, lastLogIndex, lastLogTerm)
	}

}
func (r *Raft) broadcastHeartbeat() {
	for i := range r.Prs {
		if i == r.id {
			r.Prs[i].Match = r.RaftLog.LastIndex()
			r.Prs[i].Next = r.Prs[i].Match + 1
			continue
		}
		r.sendHeartbeat(i)
	}
}
func (r *Raft) broadcastAppend() {
	for i := range r.Prs {
		if i == r.id {
			r.Prs[i].Match = r.RaftLog.LastIndex()
			r.Prs[i].Next = r.Prs[i].Match + 1
			continue
		}
		r.sendAppend(i)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = m.From

	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	lastIndex := r.RaftLog.LastIndex()
	entry := r.RaftLog.getLog(prevLogIndex)
	if entry == nil {
		r.sendAppendResponse(m.From, true, None, lastIndex)
		return
	}
	if entry.Term != prevLogTerm {
		r.sendAppendResponse(m.From, true, entry.Term, prevLogIndex)
		return
	}
	i := prevLogIndex - r.RaftLog.FirstIndex()
	DPrintf("i-%v %v %v\n", i, m.Index, m.LogTerm)
	for _, v := range m.Entries {
		if v.Index <= entry.Index {
			continue
		}
		i++
		if v.Index <= lastIndex {
			if v.Term != r.RaftLog.entries[i].Term {
				r.RaftLog.entries[i] = *v
				r.RaftLog.entries = r.RaftLog.entries[0 : i+1]
				r.RaftLog.stabled = min(r.RaftLog.stabled, v.Index-1)
			}
		} else {
			r.RaftLog.entries = append(r.RaftLog.entries, *v)
		}

	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

//func (r *Raft) handleAppendEntries(m pb.Message) {
//	// Your Code Here (2A).
//	fmt.Printf("%s has received AppendEntries,prevIndex is %v,prevTerm =%v,entries is %v.\n", r, m.Index, m.LogTerm, m.Entries)
//	DPrintf("%s log is %v\n", r, r.RaftLog.entries)
//	if m.Term != None && m.Term < r.Term {
//		r.sendAppendResponse(m.From, true, None, None)
//		return
//	}
//	r.electionElapsed = 0
//	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
//	r.Lead = m.From
//	//Step1. Conflict check
//	prevLogIndex := m.Index
//	prevLogTerm := m.LogTerm
//
//	if m.Index > r.RaftLog.LastIndex() {
//		r.sendAppendResponse(m.From, true, None, r.RaftLog.LastIndex()+1)
//		return
//	}
//	//maybe bug
//	//if m.Index == 0 {
//	//	var entries []pb.Entry
//	//	for _, v := range m.Entries {
//	//		entries = append(entries, *v)
//	//	}
//	//	r.RaftLog.entries = entries
//	//	r.RaftLog.stabled = 0
//	//	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
//	//	return
//	//}
//
//	if prevLogIndex != 0 {
//		term, _ := r.RaftLog.Term(prevLogIndex)
//		//It can be optimized. xia ci yi ding
//		if prevLogTerm != term {
//			r.sendAppendResponse(m.From, true, term, prevLogIndex)
//		}
//	}
//	//Step2. Append new entries
//	for _, v := range m.Entries {
//		if v.Index > r.RaftLog.LastIndex() {
//			break
//		}
//		logTerm, _ := r.RaftLog.Term(v.Index)
//		if logTerm != v.Term {
//			r.RaftLog.stabled = min(r.RaftLog.stabled, v.Index-1)
//			break
//		}
//
//	}
//	//rf.log = append(rf.log[0:idx+1], args.Entries...)
//	DPrintf("%s is appending new entries...\n", r)
//	if prevLogIndex != 0 {
//		idx := r.RaftLog.toSliceIndex(prevLogIndex)
//		r.RaftLog.entries = r.RaftLog.entries[0 : idx+1]
//	} else {
//		r.RaftLog.entries = []pb.Entry{}
//	}
//
//	for _, v := range m.Entries {
//		r.RaftLog.entries = append(r.RaftLog.entries, *v)
//	}
//	DPrintf("after append,log is %v", r.RaftLog.entries)
//	if m.Commit > r.RaftLog.committed {
//		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
//	}
//	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
//}

//Author: sqdbibibi Date:4.28
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	DPrintf("%s has received AE response,rej %v,index %v.\n", r, m.Reject, m.Index)
	if m.Term != None && m.Term < r.Term {
		return
	}
	if m.Reject {
		if m.Index != None {
			index := m.Index
			if m.Term != None {
				for index > r.RaftLog.FirstIndex() {
					term, _ := r.RaftLog.Term(index)
					if term == m.Term {
						index--
					}
				}
			}
			//snap bug?
			if index == r.RaftLog.FirstIndex() {
				index--
			}
			r.Prs[m.From].Next = index
			r.sendAppend(m.From)
		}
	} else {
		if m.Index > r.Prs[m.From].Match {
			r.Prs[m.From].Next = m.Index + 1
			r.Prs[m.From].Match = m.Index
			var arr []int
			for _, v := range r.Prs {
				arr = append(arr, int(v.Match))
			}
			sort.Sort(sort.Reverse(sort.IntSlice(arr)))
			DPrintf("arr is %v\n", arr)
			majorIndex := uint64(arr[len(arr)/2])
			DPrintf("%s majorIndex is %v,log is %v\n", r, majorIndex, r.RaftLog.entries)
			term, _ := r.RaftLog.Term(majorIndex)
			if term == r.Term && majorIndex > r.RaftLog.committed {
				r.RaftLog.committed = majorIndex
			}
		}

	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.Term != None && r.Term > m.Term {
		r.sendHeartbeatResponse(m.From, true)
	}
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = m.From
	r.sendHeartbeatResponse(m.From, false)
}
func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	if lastLogTerm > m.LogTerm ||
		(lastLogTerm == m.LogTerm && lastLogIndex > m.Index) {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendRequestVoteResponse(m.From, false)
}
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term != None && m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	grants := 0
	//votes := len(r.votes)
	threshold := (len(r.Prs) + 1) / 2
	for _, g := range r.votes {
		if g {
			grants++
		}
	}
	DPrintf("%s grants is %v,thd is %v\n", r, grants, threshold)
	if grants >= threshold {
		r.becomeLeader()
	}
}
func (rf *Raft) String() string {

	return fmt.Sprintf("[%s:%d;Term:%d;VotedFor:%d;logLen:%v;Commit:%v;Apply:%v]",
		stateArray[rf.State], rf.id, rf.Term, rf.Vote, len(rf.RaftLog.entries), rf.RaftLog.committed, rf.RaftLog.applied)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

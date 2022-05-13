package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState *rspb.RaftLocalState
	// current apply state of the peer
	applyState *rspb.RaftApplyState

	// current snapshot state
	snapState snap.SnapState
	// RegionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// generate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

//For Debug Date:5.12
func (ps *PeerStorage) GetInfo() *rspb.RaftApplyState {
	regionId := ps.region.GetId()
	applyState, err := meta.GetApplyState(ps.Engines.Kv, regionId)
	if err != nil {
		return nil
	}
	return applyState
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Infof("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	fmt.Printf("[newPeerStorage]%v raftState is %v.\n", tag, raftState)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	fmt.Printf("[newPeerStorage]%v applyState is %v.\n", tag, applyState)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			DPrintf("[ps.Snapshot]recv the snap.\n")
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append the given entries to the raft log and update ps.raftState also delete log entries that will
// never be committed
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	// Your Code Here (2B).
	//Author:sqdbibibi Date:5.4

	//Range check
	if len(entries) == 0 {
		return nil
	}
	lenEntrieas := len(entries)
	psFirst, _ := ps.FirstIndex()
	psLast, _ := ps.LastIndex()
	appendFirst := entries[0].Index
	appendLast := entries[lenEntrieas-1].Index

	//ignore truncated entries
	if appendFirst < psFirst {
		if appendLast < psFirst {
			return nil
		}
		entries = entries[psFirst-appendFirst:]
	}

	//append new entries
	for _, entry := range entries {
		logKey := meta.RaftLogKey(ps.region.GetId(), entry.Index)
		if err := raftWB.SetMeta(logKey, &entry); err != nil {
			return err
		}
	}

	//delete uncommited entries
	if appendLast < psLast {
		for i := appendLast + 1; i < psLast; i++ {
			logKey := meta.RaftLogKey(ps.region.GetId(), i)
			raftWB.DeleteMeta(logKey)
		}
	}
	raftWB.MustWriteToDB(ps.Engines.Raft)
	var err error
	raftWB = new(engine_util.WriteBatch)
	ps.raftState.LastIndex = appendLast
	ps.raftState.LastTerm = entries[len(entries)-1].Term
	raftWB, err = ps.SetRaftstate(ps.raftState, raftWB)
	if err != nil {
		return err
	}
	raftWB.MustWriteToDB(ps.Engines.Raft)
	return nil
}

// Apply the peer with given snapshot
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// Your Code Here (2C).
	// Author:sqdbibibi Date:5.4
	fmt.Printf("[applySnap]%v. sK is %v,eK is %v,ps.region is %v.\n", ps.Tag, snapData.Region.StartKey, snapData.Region.EndKey, ps.region)
	if ps.isInitialized() {
		//if err := ps.clearMeta(kvWB, raftWB); err != nil {
		//	return nil, err
		//}
		ps.clearExtraData(snapData.Region)
		raftWB.MustWriteToDB(ps.Engines.Raft)
		raftWB = new(engine_util.WriteBatch)
		kvWB.MustWriteToDB(ps.Engines.Kv)
		kvWB = new(engine_util.WriteBatch)
	}

	ps.snapState.StateType = snap.SnapState_Applying
	ps.raftState.LastIndex = snapshot.Metadata.Index
	ps.raftState.LastTerm = snapshot.Metadata.Term
	ps.applyState.TruncatedState.Index = snapshot.Metadata.Index
	ps.applyState.TruncatedState.Term = snapshot.Metadata.Term
	ps.applyState.AppliedIndex = snapshot.Metadata.Index
	meta.WriteRegionState(kvWB, snapData.Region, rspb.PeerState_Normal)

	raftWB.SetMeta(meta.RaftStateKey(snapData.Region.GetId()), ps.raftState)
	raftWB.MustWriteToDB(ps.Engines.Raft)
	raftWB = new(engine_util.WriteBatch)

	kvWB.SetMeta(meta.ApplyStateKey(snapData.Region.GetId()), ps.applyState)
	kvWB.MustWriteToDB(ps.Engines.Kv)
	kvWB = new(engine_util.WriteBatch)

	apSt0, e := meta.GetApplyState(ps.Engines.Kv, snapData.Region.GetId())
	if e != nil {
		apSt0 = nil
	}
	fmt.Printf("[applySnap]before,key is %v,ps.ap is %v,appSt is %v.\n", meta.ApplyStateKey(snapData.Region.GetId()), ps.applyState, apSt0)

	// schedule snapshot apply task
	ch := make(chan bool, 1)
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: snapData.Region.GetId(),
		Notifier: ch,
		SnapMeta: snapshot.Metadata,
		StartKey: snapData.Region.GetStartKey(),
		EndKey:   snapData.Region.GetEndKey(),
	}
	<-ch
	res := &ApplySnapResult{PrevRegion: ps.region, Region: snapData.Region}

	apSt1, e := meta.GetApplyState(ps.Engines.Kv, snapData.Region.GetId())
	if e != nil {
		apSt1 = nil
	}
	fmt.Printf("[applySnap]key is %v,ps.ap is %v,appSt is %v.\n", meta.ApplyStateKey(snapData.Region.GetId()), ps.applyState, apSt1)

	kvWB.SetMeta(meta.ApplyStateKey(snapData.Region.GetId()), ps.applyState)
	kvWB.MustWriteToDB(ps.Engines.Kv)
	kvWB = new(engine_util.WriteBatch)

	apSt2, e := meta.GetApplyState(ps.Engines.Kv, snapData.Region.GetId())
	if e != nil {
		apSt2 = nil
	}
	fmt.Printf("[applySnap]after,%v. sK is %v,eK is %v,region is %v,apSt is %v\n", ps.Tag, ps.region.StartKey, ps.region.EndKey, ps.region, apSt2)

	return res, nil
}

//Author:sqdbibibi Date:5.4 Des: Store new RaftState to RaftDB.
func (ps *PeerStorage) SetRaftstate(raftstate *rspb.RaftLocalState, raftWB *engine_util.WriteBatch) (*engine_util.WriteBatch, error) {
	if err := raftWB.SetMeta(meta.RaftStateKey(ps.region.GetId()), raftstate); err != nil {
		return nil, err
	}
	return raftWB, nil
}
func (ps *PeerStorage) SetApplystate(applystate *rspb.RaftApplyState, kvWB *engine_util.WriteBatch) (*engine_util.WriteBatch, error) {
	if err := kvWB.SetMeta(meta.ApplyStateKey(ps.region.GetId()), applystate); err != nil {
		return nil, err
	}
	return kvWB, nil
}

// Save memory states to disk.
// Do not modify ready in this function, this is a requirement to advance the ready object properly later.
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	// Your Code Here (2B/2C).
	//Author:sqdbibibi Date:5.3
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	snapshot := ready.Snapshot
	entries := ready.Entries
	hardSt := ready.HardState
	var applySanpResult *ApplySnapResult
	if !raft.IsEmptySnap(&snapshot) {
		if res, err := ps.ApplySnapshot(&snapshot, kvWB, raftWB); err != nil {
			return nil, err
		} else {
			applySanpResult = res
		}
	}

	if err := ps.Append(entries, raftWB); err != nil {
		panic(err)
	}

	if !raft.IsEmptyHardState(hardSt) {
		ps.raftState.HardState = &hardSt
	}
	var err error
	raftWB = new(engine_util.WriteBatch)
	if raftWB, err = ps.SetRaftstate(ps.raftState, raftWB); err != nil {
		log.Errorf("[SaveReadyState]Apply the RaftState fail ,err is %v", err)
		return nil, err
	}

	kvWB.MustWriteToDB(ps.Engines.Kv)
	raftWB.MustWriteToDB(ps.Engines.Raft)

	return applySanpResult, nil
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	fmt.Printf("[clearRange]is executing...\n")
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}

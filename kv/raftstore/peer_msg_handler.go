package raftstore

import (
	"fmt"
	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
	"reflect"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		fmt.Printf(format, a...)
	}
	return
}

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

//Author:sqdbibibi Date:5.4 Des:After applySnap, region may change.
func (d *peerMsgHandler) handleRegionChange(prevRegion *metapb.Region, region *metapb.Region) {
	fmt.Printf("[handleRegionChange]prevRegion: %v region: %v", prevRegion, region)
	d.peerStorage.SetRegion(region)
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	defer storeMeta.Unlock()
	storeMeta.regions[region.Id] = region
	storeMeta.regionRanges.Delete(&regionItem{prevRegion})
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region})
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()
		res, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic(err)
		}
		if res != nil {
			if !reflect.DeepEqual(res.PrevRegion, res.Region) {
				d.handleRegionChange(res.PrevRegion, res.Region)
			}
		}
		//fmt.Printf("[handleRaftReady]%srd.msgs is %v\n", d.RaftGroup.Raft, rd.Messages)
		d.Send(d.ctx.trans, rd.Messages)
		if len(rd.CommittedEntries) > 0 {

			kvWB := new(engine_util.WriteBatch)
			for _, entry := range rd.CommittedEntries {
				kvWB = d.applyEntry(&entry, kvWB)
			}
			kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
		}
		d.RaftGroup.Advance(rd)
	}

}

// Author:sqdbibibi Date:5.5 Des:handle proposal for every state.
func (d *peerMsgHandler) handleProposal(entry *eraftpb.Entry, handle func(*proposal)) {
	tries := 0
	for len(d.proposals) > 0 {
		tries++
		if tries >= 10 {
			panic("try 10 times")
		}
		fmt.Printf("[handleProposal] r is %v idx %v,term %v,len(ps) %v\n", d.RaftGroup.Raft, entry.Index, entry.Term, len(d.proposals))
		for idx, v := range d.proposals {
			fmt.Printf("%v:%v", idx, v)
		}
		proposal := d.proposals[0]
		if entry.Term < proposal.term {
			return
		}
		if entry.Term > proposal.term {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}
		if entry.Index == proposal.index && entry.Term < proposal.term {
			return
		}
		if entry.Index == proposal.index && entry.Term > proposal.term {
			proposal.cb.Done(ErrRespStaleCommand(proposal.term))
			d.proposals = d.proposals[1:]
			continue
		}
		if entry.Index == proposal.index && entry.Term == proposal.term {

			handle(proposal)
			// Author:sqdbibibi Date:5.12 Des:delete same proposals.
			for len(d.proposals) > 1 {
				if proposal.index == d.proposals[1].index && proposal.term == d.proposals[1].term {
					d.proposals = d.proposals[1:]
				} else {
					break
				}
			}
			d.proposals = d.proposals[1:]

			return
		}
	}
}

// Author:sqdbibibi Date:5.4
func (d *peerMsgHandler) applyNormalRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	DPrintf("%s apply for %v Request.\n", d.RaftGroup.Raft, msg.Requests[0].CmdType)

	req := msg.Requests[0]
	key := getRequestKey(msg.Requests[0])
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return kvWB
		}
	}

	switch req.CmdType {
	case raft_cmdpb.CmdType_Put:
		kvWB.SetCF(req.Put.GetCf(), req.Put.GetKey(), req.Put.Value)
	case raft_cmdpb.CmdType_Get:
	case raft_cmdpb.CmdType_Delete:
		kvWB.DeleteCF(req.Delete.GetCf(), req.Delete.GetKey())
	case raft_cmdpb.CmdType_Snap:
	}
	kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	kvWB = new(engine_util.WriteBatch)

	var err error
	d.handleProposal(entry, func(p *proposal) {
		rsp := raft_cmdpb.RaftCmdResponse{Header: &raft_cmdpb.RaftResponseHeader{}}

		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			DPrintf("%s is making PUT response.\n", d.RaftGroup.Raft)
			rsp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Put, Put: &raft_cmdpb.PutResponse{}}}
		case raft_cmdpb.CmdType_Get:
			val, e := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.GetCf(), req.Get.Key)
			if e != nil {
				val = nil
			}
			rsp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Get, Get: &raft_cmdpb.GetResponse{Value: val}}}
		case raft_cmdpb.CmdType_Delete:
			rsp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Delete, Delete: &raft_cmdpb.DeleteResponse{}}}
		case raft_cmdpb.CmdType_Snap:
			if msg.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
				p.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				return
			}
			rsp.Responses = []*raft_cmdpb.Response{{CmdType: raft_cmdpb.CmdType_Snap, Snap: &raft_cmdpb.SnapResponse{Region: d.Region()}}}
			p.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
		}
		d.peerStorage.applyState.AppliedIndex = entry.Index
		kvWB, err = d.peerStorage.SetApplystate(d.peerStorage.applyState, kvWB)
		if err != nil {
			panic(err)
		}
		kvWB.WriteToDB(d.peerStorage.Engines.Kv)

		p.cb.Done(&rsp)
	})
	kvWB = new(engine_util.WriteBatch)
	DPrintf("%s apply for %v Request finish.\n", d.RaftGroup.Raft, msg.Requests[0].CmdType)

	return kvWB
}

// Author:sqdbibibi Date:5.6
func (d *peerMsgHandler) applyAdminRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	req := msg.AdminRequest
	var err error
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		applySt := d.peerStorage.applyState
		compactLog := msg.AdminRequest.GetCompactLog()
		if applySt.TruncatedState.Index <= compactLog.CompactIndex {
			applySt.TruncatedState.Index = compactLog.CompactIndex
			applySt.TruncatedState.Term = compactLog.CompactTerm
			applySt.AppliedIndex = entry.Index
			kvWB, err = d.peerStorage.SetApplystate(d.peerStorage.applyState, kvWB)
			if err != nil {
				panic(err)
			}

			d.ScheduleCompactLog(d.peerStorage.applyState.TruncatedState.Index)
		}
	case raft_cmdpb.AdminCmdType_Split:
		region := d.Region()
		err := util.CheckRegionEpoch(msg, region, true)
		if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(errEpochNotMatching))
			})
			return kvWB
		}
		split := req.GetSplit()
		err = util.CheckKeyInRegion(split.SplitKey, region)
		if err != nil {
			d.handleProposal(entry, func(p *proposal) {
				p.cb.Done(ErrResp(err))
			})
			return kvWB
		}
		region.RegionEpoch.Version++
		if len(split.NewPeerIds) != len(region.Peers) {
			fmt.Printf("Different length of peers,reject split!")
			return kvWB
		}
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()

		storeMeta.regionRanges.Delete(&regionItem{region: region})
		var peers []*metapb.Peer
		for i, peerMeta := range region.Peers {
			peers = append(peers, &metapb.Peer{Id: split.NewPeerIds[i], StoreId: peerMeta.StoreId})
		}
		newRegion := &metapb.Region{
			Id:       split.NewRegionId,
			StartKey: split.SplitKey,
			EndKey:   region.EndKey,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: peers,
		}
		region.EndKey = split.SplitKey
		storeMeta.regions[newRegion.Id] = newRegion
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})

		storeMeta.Unlock()

		meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
		meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
		d.SizeDiffHint = 0
		d.ApproximateSize = new(uint64)
		peer, err := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
		if err != nil {
			panic(err)
		}
		storeMeta.Lock()
		storeMeta.setRegion(newRegion, peer)
		storeMeta.Unlock()

		d.ctx.router.register(peer)
		d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
		d.handleProposal(entry, func(p *proposal) {
			p.cb.Done(&raft_cmdpb.RaftCmdResponse{
				Header: &raft_cmdpb.RaftResponseHeader{},
				AdminResponse: &raft_cmdpb.AdminResponse{
					CmdType: raft_cmdpb.AdminCmdType_Split,
					Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{region, newRegion}},
				},
			})
		})

		d.notifyHeartbeatScheduler(region, d.peer)
		d.notifyHeartbeatScheduler(newRegion, peer)
		if d.IsLeader() {
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	}
	kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
	kvWB = new(engine_util.WriteBatch)
	return kvWB
}

// Author:sqdbibibi Date:5.8 Des:Search the idx for a given peerId in region.
func searchPeerInRegion(peerId uint64, region *metapb.Region) int {
	for i, _ := range region.Peers {
		if region.Peers[i].Id == peerId {
			return i
		}
	}
	return len(region.Peers)
}

// Author:sqdbibibi Date:5.8
func (d *peerMsgHandler) applyConfChangeRequest(entry *eraftpb.Entry, cc eraftpb.ConfChange, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	msg := &raft_cmdpb.RaftCmdRequest{}
	err := msg.Unmarshal(cc.Context)
	if err != nil {
		panic(err)
	}
	region := d.Region()
	err = util.CheckRegionEpoch(msg, region, true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		d.handleProposal(entry, func(p *proposal) {
			p.cb.Done(ErrResp(errEpochNotMatching))
		})
		return kvWB
	}
	fmt.Printf("[applyConfChange ###%v] r is %v\n", cc.GetChangeType(), d.RaftGroup.Raft)
	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		idx := searchPeerInRegion(cc.NodeId, region)
		if idx == len(region.Peers) {
			//fmt.Printf("[apply-addNode]add %v\n", cc.NodeId)
			peer := msg.AdminRequest.ChangePeer.Peer
			region.Peers = append(region.Peers, peer)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.insertPeerCache(peer)
		}

	case eraftpb.ConfChangeType_RemoveNode:
		if cc.NodeId == d.Meta.Id {
			if len(d.Region().Peers) == 2 && d.IsLeader() {
				var targetPeer uint64 = 0
				for _, peer := range d.Region().Peers {
					if peer.Id != d.PeerId() {
						targetPeer = peer.Id
						break
					}
				}
				if targetPeer == 0 {
					panic("This should not happen")
				}
				m := []eraftpb.Message{{
					To:      targetPeer,
					MsgType: eraftpb.MessageType_MsgHeartbeat,
					Commit:  d.peerStorage.raftState.HardState.Commit,
				}}
				for i := 0; i < 10; i++ {
					d.Send(d.ctx.trans, m)
					time.Sleep(100 * time.Millisecond)
				}
			}
			d.destroyPeer()
			return kvWB
		}

		idx := searchPeerInRegion(cc.NodeId, region)
		if idx < len(region.Peers) {
			region.Peers = append(region.Peers[:idx], region.Peers[idx+1:]...)
			region.RegionEpoch.ConfVer++
			meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
			storeMeta := d.ctx.storeMeta
			storeMeta.Lock()
			storeMeta.regions[region.Id] = region
			storeMeta.Unlock()
			d.removePeerCache(cc.NodeId)
		}
	}
	d.RaftGroup.ApplyConfChange(cc)

	kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	kvWB = new(engine_util.WriteBatch)

	d.handleProposal(entry, func(p *proposal) {
		p.cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{},
			},
		})
	})
	d.notifyHeartbeatScheduler(d.Region(), d.peer)
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return kvWB
}

// Author:sqdbibibi Date:5.4 Desp:Apply a single entry to kvDB
func (d *peerMsgHandler) applyEntry(entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	//Conf change entry
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := eraftpb.ConfChange{}
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			panic(err)
		}
		kvWB = d.applyConfChangeRequest(entry, cc, kvWB)
		return kvWB
	}

	//Normal entry
	msg := &raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(entry.Data); err != nil {
		panic(err)
	}

	if msg.AdminRequest == nil {
		if len(msg.Requests) > 0 {
			kvWB = d.applyNormalRequest(entry, msg, kvWB)
		}
	} else {
		kvWB = d.applyAdminRequest(entry, msg, kvWB)
	}
	return kvWB
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}
func getRequestKey(req *raft_cmdpb.Request) []byte {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	}
	return key
}

func (d *peerMsgHandler) proposeNormalRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	DPrintf("%s propose %v Request.\n", d.RaftGroup.Raft, msg.Requests[0].CmdType)
	key := getRequestKey(msg.Requests[0])
	if key != nil {
		err := util.CheckKeyInRegion(key, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
	}
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
	d.proposals = append(d.proposals, p)
	d.RaftGroup.Propose(data)
}

// Author:sqdbibibi Date:5.6 5.8
func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.AdminRequest
	data, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.RaftGroup.Propose(data)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		transferee := msg.AdminRequest.TransferLeader.Peer
		d.RaftGroup.TransferLeader(transferee.GetId())
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			},
		})
		DPrintf("%s propose %v AdminRequest, transferee is %v\n", d.RaftGroup.Raft, msg.AdminRequest.CmdType, transferee)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		confChange := eraftpb.ConfChange{
			ChangeType: req.ChangePeer.ChangeType,
			NodeId:     req.ChangePeer.Peer.Id,
			Context:    data}

		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.ProposeConfChange(confChange)
	case raft_cmdpb.AdminCmdType_Split:
		err := util.CheckKeyInRegion(req.Split.SplitKey, d.Region())
		if err != nil {
			cb.Done(ErrResp(err))
			return
		}
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}
		p := &proposal{index: d.nextProposalIndex(), term: d.Term(), cb: cb}
		d.proposals = append(d.proposals, p)
		d.RaftGroup.Propose(data)
	}

}
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// Author:sqdbibibi Date:5.4
	if msg.AdminRequest == nil {
		if len(msg.Requests) > 0 {
			d.proposeNormalRequest(msg, cb)
		}
	} else {
		d.proposeAdminRequest(msg, cb)
	}

}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy, raft is %v", d.Tag, d.RaftGroup.Raft)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

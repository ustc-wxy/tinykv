// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeSlice []*core.StoreInfo

func (a storeSlice) Len() int           { return len(a) }
func (a storeSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a storeSlice) Less(i, j int) bool { return a[i].GetRegionSize() < a[j].GetRegionSize() }
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	suitableStores := make(storeSlice, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	sum := len(suitableStores)
	if sum < 2 {
		return nil
	}
	sort.Sort(suitableStores)
	var storeFrom *core.StoreInfo
	var storeTo *core.StoreInfo

	var regionFrom *core.RegionInfo
	var candidateRegions core.RegionsContainer
	var idxFrom int
	for i := sum - 1; i > 0; i-- {
		store := suitableStores[i]

		cluster.GetPendingRegionsWithLock(store.GetID(), func(rc core.RegionsContainer) { candidateRegions = rc })
		if regionFrom = candidateRegions.RandomRegion(nil, nil); regionFrom != nil {
			storeFrom = store
			idxFrom = i
			break
		}

		cluster.GetFollowersWithLock(store.GetID(), func(rc core.RegionsContainer) { candidateRegions = rc })
		if regionFrom = candidateRegions.RandomRegion(nil, nil); regionFrom != nil {
			storeFrom = store
			idxFrom = i
			break
		}

		cluster.GetLeadersWithLock(store.GetID(), func(rc core.RegionsContainer) { candidateRegions = rc })
		if regionFrom = candidateRegions.RandomRegion(nil, nil); regionFrom != nil {
			storeFrom = store
			idxFrom = i
			break
		}
	}

	if regionFrom == nil {
		return nil
	}

	storeIds := regionFrom.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}

	for i := 0; i < idxFrom; i++ {
		if _, ok := storeIds[suitableStores[i].GetID()]; !ok {
			storeTo = suitableStores[i]
			break
		}
	}

	if storeTo == nil {
		return nil
	}
	if storeFrom.GetRegionSize()-storeTo.GetRegionSize() < 2*regionFrom.GetApproximateSize() {
		return nil
	}
	newPeer, err := cluster.AllocPeer(storeTo.GetID())
	if err != nil {
		return nil
	}
	//fmt.Printf("[Schedule]move-from-%d-to-%d\n", storeFrom.GetID(), storeTo.GetID())
	desc := fmt.Sprintf("move-from-%d-to-%d", storeFrom.GetID(), storeTo.GetID())
	op, err := operator.CreateMovePeerOperator(desc, cluster, regionFrom, operator.OpBalance, storeFrom.GetID(), storeTo.GetID(), newPeer.GetId())
	if err != nil {
		return nil
	}

	return op
}

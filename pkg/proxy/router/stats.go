// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"encoding/json"
	"github.com/wandoulabs/codis/pkg/utils/atomic2"
	"sync"
	"time"
)

type OpStats struct {
	opstr string
	calls atomic2.Int64
	usecs atomic2.Int64
}

type SlowOpInfo struct {
	Time     string
	Duration int64
	Key      string
	Reqs     int64
}

func (s *OpStats) OpStr() string {
	return s.opstr
}

func (s *OpStats) Calls() int64 {
	return s.calls.Get()
}

func (s *OpStats) USecs() int64 {
	return s.usecs.Get()
}

func (s *OpStats) MarshalJSON() ([]byte, error) {
	var m = make(map[string]interface{})
	var calls = s.calls.Get()
	var usecs = s.usecs.Get()

	var perusecs int64 = 0
	if calls != 0 {
		perusecs = usecs / calls
	}

	m["cmd"] = s.opstr
	m["calls"] = calls
	m["usecs"] = usecs
	m["usecs_percall"] = perusecs
	return json.Marshal(m)
}

var cmdstats struct {
	requests atomic2.Int64

	opmap        map[string]*OpStats
	rwlck        sync.RWMutex
	slowOps      []*SlowOpInfo
	slowoplk     sync.Mutex
	lastLogUsecs int64
}

func init() {
	cmdstats.opmap = make(map[string]*OpStats)
	cmdstats.slowOps = make([]*SlowOpInfo, 0, 20)
	cmdstats.lastLogUsecs = 0
	go func() {
		for {
			for {
				slen := len(cmdstats.slowOps)
				if slen < 10 {
					break
				}
				cmdstats.slowoplk.Lock()
				if slen < 50 {
					cmdstats.slowOps = cmdstats.slowOps[1:]
				} else {
					cmdstats.slowOps = cmdstats.slowOps[slen-1:]
				}
				cmdstats.slowoplk.Unlock()

			}
			time.Sleep(5 * time.Second)
		}
	}()
}

func OpCounts() int64 {
	return cmdstats.requests.Get()
}

func GetOpStats(opstr string, create bool) *OpStats {
	cmdstats.rwlck.RLock()
	s := cmdstats.opmap[opstr]
	cmdstats.rwlck.RUnlock()

	if s != nil || !create {
		return s
	}

	cmdstats.rwlck.Lock()
	s = cmdstats.opmap[opstr]
	if s == nil {
		s = &OpStats{opstr: opstr}
		cmdstats.opmap[opstr] = s
	}
	cmdstats.rwlck.Unlock()
	return s
}

func GetAllOpStats() []*OpStats {
	var all = make([]*OpStats, 0, 128)
	cmdstats.rwlck.RLock()
	for _, s := range cmdstats.opmap {
		all = append(all, s)
	}
	cmdstats.rwlck.RUnlock()
	return all
}

func incrOpStats(opstr string, usecs int64) {
	s := GetOpStats(opstr, true)
	s.calls.Incr()
	s.usecs.Add(usecs)
	cmdstats.requests.Incr()
}

func addSlowOps(time string, key string, usecs int64) {
	s := &SlowOpInfo{
		Time:     time,
		Key:      key,
		Duration: usecs,
		Reqs:     cmdstats.requests.Get(),
	}
	cmdstats.slowOps = append(cmdstats.slowOps, s)
}

func GetSlowOps() []*SlowOpInfo {
	var all = make([]*SlowOpInfo, 0, 10)
	cmdstats.slowoplk.Lock()
	for _, val := range cmdstats.slowOps {
		all = append(all, val)
	}
	cmdstats.slowoplk.Unlock()
	return all
}

func (s *SlowOpInfo) MarshalJSON() ([]byte, error) {
	var m = make(map[string]interface{})
	m["key"] = s.Key
	m["duration"] = s.Duration
	m["time"] = s.Time
	m["req"] = s.Reqs
	return json.Marshal(m)
}

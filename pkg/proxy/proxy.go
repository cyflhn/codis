// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/wandoulabs/codis/pkg/models"
	"github.com/wandoulabs/codis/pkg/proxy/router"
	"github.com/wandoulabs/codis/pkg/utils/log"
	topo "github.com/wandoulabs/go-zookeeper/zk"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SERVER_STATUS_STOP     = 0
	SERVER_STATUS_STARTING = 1
	SERVER_STATUS_STARTED  = 2
)

type Server struct {
	conf   *Config
	topo   *Topology
	info   models.ProxyInfo
	groups map[int]int

	lastActionSeq int

	evtbus   chan interface{}
	router   *router.Router
	listener net.Listener

	kill      chan interface{}
	wait      sync.WaitGroup
	stop      sync.Once
	startLock sync.Mutex
	status    int //
}

func New(addr string, debugVarAddr string, conf *Config) *Server {
	log.Infof("create proxy with config: %+v", conf)

	proxyHost := strings.Split(addr, ":")[0]
	debugHost := strings.Split(debugVarAddr, ":")[0]

	hostname, err := os.Hostname()
	if err != nil {
		log.PanicErrorf(err, "get host name failed")
	}
	if proxyHost == "0.0.0.0" || strings.HasPrefix(proxyHost, "127.0.0.") {
		proxyHost = hostname
	}
	if debugHost == "0.0.0.0" || strings.HasPrefix(debugHost, "127.0.0.") {
		debugHost = hostname
	}

	s := &Server{conf: conf, lastActionSeq: -1, groups: make(map[int]int)}
	s.topo = NewTopo(conf.productName, conf.zkAddr, conf.fact, conf.provider, conf.zkSessionTimeout)
	s.info.Id = conf.proxyId
	s.info.State = models.PROXY_STATE_OFFLINE
	s.info.Addr = proxyHost + ":" + strings.Split(addr, ":")[1]
	s.info.DebugVarAddr = debugHost + ":" + strings.Split(debugVarAddr, ":")[1]
	s.info.Pid = os.Getpid()
	s.info.StartAt = time.Now().String()
	s.kill = make(chan interface{})

	log.Infof("proxy info = %+v", s.info)

	if l, err := net.Listen(conf.proto, addr); err != nil {
		log.PanicErrorf(err, "open listener failed")
	} else {
		s.listener = l
	}
	s.router = router.NewWithAuth(conf.passwd)
	s.evtbus = make(chan interface{}, 1024)

	s.register()

	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		s.serve()
	}()
	return s
}

func (s *Server) SetMyselfOnline() error {
	log.Info("mark myself online")
	info := models.ProxyInfo{
		Id:    s.conf.proxyId,
		State: models.PROXY_STATE_ONLINE,
	}
	b, _ := json.Marshal(info)
	url := "http://" + s.conf.dashboardAddr + "/api/proxy"
	res, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New("response code is not 200")
	}
	return nil
}

func (s *Server) serve() {
	defer s.close()

	if !s.waitOnline() {
		return
	}

	s.rewatchNodes()

	s.fillSlots()

	go func() {
		defer s.close()
		s.handleConns()
	}()

	s.loopEvents()
}

func (s *Server) handleConns() {
	ch := make(chan net.Conn, 4096)
	defer close(ch)

	go func() {
		for c := range ch {
			x := router.NewSessionSize(c, s.conf.passwd, s.conf.maxBufSize, s.conf.maxTimeout)
			go x.Serve(s.router, s.conf.maxPipeline)
		}
	}()

	for {
		c, err := s.listener.Accept()
		if err != nil {
			return
		} else {
			/*
				if s.status == SERVER_STATUS_STARTING {
					s.listener.Close()
					for {
						if s.status == SERVER_STATUS_STARTED {
							if l, err := net.Listen("tcp", ":"+strings.Split(s.info.Addr, ":")[1]); err != nil {
								log.ErrorErrorf(err, "open listener failed")
								time.Sleep(5 * time.Second)
							} else {
								s.listener = l
								break
							}
						} else {
							time.Sleep(5 * time.Second)
						}
					}
				} else {
					ch <- c
				}
			*/
			ch <- c
		}
	}
}

func (s *Server) Info() models.ProxyInfo {
	return s.info
}

func (s *Server) Join() {
	s.wait.Wait()
}

func (s *Server) Close() error {
	s.close()
	s.wait.Wait()
	return nil
}

func (s *Server) close() {
	s.stop.Do(func() {
		s.listener.Close()
		if s.router != nil {
			s.router.Close()
		}
		close(s.kill)
	})
}

func (s *Server) rewatchProxy() {
	var err error
	for {
		_, err = s.topo.WatchNode(path.Join(models.GetProxyPath(s.topo.ProductName), s.info.Id), s.evtbus)
		if err != nil {
			log.ErrorErrorf(err, "watch node failed")
			if s.topo.IsFatalErr(err) {
				s.reRegister(models.PROXY_STATE_ONLINE)
			} else {
				time.Sleep(5 * time.Second)
			}
		} else {
			break
		}
	}
}

func (s *Server) rewatchNodes() []string {
	var nodes []string
	var err error
	for {
		nodes, err = s.topo.WatchChildren(models.GetWatchActionPath(s.topo.ProductName), s.evtbus)
		if err != nil {
			log.ErrorErrorf(err, "watch children failed")
			if s.topo.IsFatalErr(err) {
				s.reRegisterAndReWatchPrxoy(models.PROXY_STATE_ONLINE)
			} else {
				time.Sleep(5 * time.Second)
			}
		} else {
			break
		}
	}
	return nodes
}

func (s *Server) register() {
	var err error
	for {
		if _, err = s.topo.CreateProxyInfo(&s.info); err != nil {
			log.ErrorErrorf(err, "create proxy node failed")
			if s.topo.IsErrSessionExpired(err) {
				s.topo.RefreshZkConn()
			}
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		} else {
			break
		}
	}

	for {
		if _, err = s.topo.CreateProxyFenceNode(&s.info); err != nil {
			log.ErrorErrorf(err, "create fence node failed")
			if s.topo.IsErrSessionExpired(err) {
				s.topo.RefreshZkConn()
			} else if s.topo.IsErrNodeExist(err) {
				break
			}
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		} else {
			break
		}
	}
}

func (s *Server) markOffline() {
	s.topo.Close(s.info.Id)
	s.info.State = models.PROXY_STATE_MARK_OFFLINE
}

func (s *Server) waitOnline() bool {
	for {
		info, err := s.topo.GetProxyInfo(s.info.Id)
		if err != nil {
			log.ErrorErrorf(err, "get proxy info failed: %s", s.info.Id)
			if s.topo.IsFatalErr(err) {
				s.reRegister(models.PROXY_STATE_MARK_OFFLINE)
			}
			continue
		}
		switch info.State {
		case models.PROXY_STATE_MARK_OFFLINE:
			log.Infof("mark offline, proxy got offline event: %s", s.info.Id)
			s.markOffline()
			return false
		case models.PROXY_STATE_ONLINE:
			s.info.State = info.State
			log.Infof("we are online: %s", s.info.Id)
			s.rewatchProxy()
			return true
		}
		select {
		case <-s.kill:
			log.Infof("mark offline, proxy is killed: %s", s.info.Id)
			s.markOffline()
			return false
		default:
		}
		log.Infof("wait to be online: %s", s.info.Id)
		time.Sleep(3 * time.Second)
	}
}

func getEventPath(evt interface{}) string {
	return evt.(topo.Event).Path
}

func needResponse(receivers []string, self models.ProxyInfo) bool {
	var info models.ProxyInfo
	for _, v := range receivers {
		err := json.Unmarshal([]byte(v), &info)
		if err != nil {
			if v == self.Id {
				return true
			}
			return false
		}
		if info.Id == self.Id && info.Pid == self.Pid && info.StartAt == self.StartAt {
			return true
		}
	}
	return false
}

func groupMaster(groupInfo models.ServerGroup) string {
	var master string
	for _, server := range groupInfo.Servers {
		if server.Type == models.SERVER_TYPE_MASTER {
			if master != "" {
				log.Errorf("two master not allowed: %+v", groupInfo)
			}
			master = server.Addr
		}
	}
	if master == "" {
		log.Errorf("master not found: %+v", groupInfo)
	}
	return master
}

func (s *Server) resetSlot(i int) {
	s.router.ResetSlot(i)
}

/*
* return err should be reconnect to zk
*
 */
func (s *Server) fillSlot(i int) error {
	slotInfo, slotGroup, err := s.topo.GetSlotByIndex(i)
	if err != nil {
		log.ErrorErrorf(err, "get slot by index failed", i)
		return err
	}

	var from string
	var addr = groupMaster(*slotGroup)
	if slotInfo.State.Status == models.SLOT_STATUS_MIGRATE {
		fromGroup, err := s.topo.GetGroup(slotInfo.State.MigrateStatus.From)
		if err != nil {
			log.ErrorErrorf(err, "get migrate from failed")
			return err
		}

		from = groupMaster(*fromGroup)
		if from == addr {
			log.Errorf("set slot %04d migrate from %s to %s", i, from, addr)
			return nil
		}

		if "" == addr {
			log.Errorf("set slot %04d addr nil", i)
			return nil
		}
	}

	s.groups[i] = slotInfo.GroupId
	s.router.FillSlot(i, addr, from,
		slotInfo.State.Status == models.SLOT_STATUS_PRE_MIGRATE)
	return err
}

func (s *Server) onSlotRangeChange(param *models.SlotMultiSetParam) error {
	log.Infof("slotRangeChange %+v", param)
	for i := param.From; i <= param.To; i++ {
		switch param.Status {
		case models.SLOT_STATUS_OFFLINE:
			s.resetSlot(i)
		case models.SLOT_STATUS_ONLINE:
			if err := s.fillSlot(i); err != nil {
				//s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
				return err
			}
		default:
			log.Errorf("can not handle status %v", param.Status)
		}
	}
	return nil
}

func (s *Server) onGroupChange(groupId int) error {
	log.Infof("group changed %d", groupId)
	for i, g := range s.groups {
		if g == groupId {
			if err := s.fillSlot(i); err != nil {
				//s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
				return err
			}
		}
	}
	return nil
}

func (s *Server) responseAction(seq int64) {
	log.Infof("send response seq = %d", seq)
	err := s.topo.DoResponse(int(seq), &s.info)
	if err != nil {
		log.InfoErrorf(err, "send response seq = %d failed", seq)
	}
}

func (s *Server) getActionObject(seq int, target interface{}) error {
	act := &models.Action{Target: target}
	err := s.topo.GetActionWithSeqObject(int64(seq), act)
	if err != nil {
		log.ErrorError(err, "get action object failed, seq = %d", seq)
	}
	log.Infof("action %+v", act)
	return err
}

func (s *Server) checkAndDoTopoChange(seq int) bool {
	act, err := s.topo.GetActionWithSeq(int64(seq))
	if err != nil {
		//s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
		return false
	}
	if !needResponse(act.Receivers, s.info) { //no need to response
		return false
	}

	log.Warnf("action %v receivers %v", seq, act.Receivers)

	switch act.Type {
	case models.ACTION_TYPE_SLOT_MIGRATE, models.ACTION_TYPE_SLOT_CHANGED,
		models.ACTION_TYPE_SLOT_PREMIGRATE:
		slot := &models.Slot{}
		if err := s.getActionObject(seq, slot); err != nil {
			return false
		}
		if err := s.fillSlot(slot.Id); err != nil {
			//s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
			return false
		}
	case models.ACTION_TYPE_SERVER_GROUP_CHANGED:
		serverGroup := &models.ServerGroup{}
		if err := s.getActionObject(seq, serverGroup); err != nil {
			return false
		}
		if err := s.onGroupChange(serverGroup.Id); err != nil {
			return false
		}
	case models.ACTION_TYPE_SERVER_GROUP_REMOVE:
	//do not care
	case models.ACTION_TYPE_MULTI_SLOT_CHANGED:
		param := &models.SlotMultiSetParam{}
		if err := s.getActionObject(seq, param); err != nil {
			return false
		}
		if err := s.onSlotRangeChange(param); err != nil {
			return false
		}
	default:
		log.Errorf("unknown action %+v", act)
	}
	return true
}

func (s *Server) processAction(e interface{}) error {
	if s.topo.IsSessionExpiredEvent(e) {
		//s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
		return topo.ErrSessionExpired
	}
	if strings.Index(getEventPath(e), models.GetProxyPath(s.topo.ProductName)) == 0 {
		info, err := s.topo.GetProxyInfo(s.info.Id)
		if err != nil {
			log.ErrorErrorf(err, "get proxy info failed: %s", s.info.Id)
			//s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
			return err
		}
		switch info.State {
		case models.PROXY_STATE_MARK_OFFLINE:
			log.Infof("mark offline, proxy got offline event: %s", s.info.Id)
			s.markOffline()
		case models.PROXY_STATE_ONLINE:
			s.rewatchProxy()
		default:
			log.Errorf("unknown proxy state %+v", info)
		}
		return nil
	}

	//re-watch
	nodes := s.rewatchNodes()

	seqs, err := models.ExtraSeqList(nodes)
	if err != nil {
		log.ErrorErrorf(err, "get seq list failed")
		//s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
		return err
	}

	if len(seqs) == 0 || !s.topo.IsChildrenChangedEvent(e) {
		return nil
	}

	//get last pos
	index := -1
	for i, seq := range seqs {
		if s.lastActionSeq < seq {
			index = i
			break
		}
	}

	if index < 0 {
		return nil
	}

	actions := seqs[index:]
	for _, seq := range actions {
		exist, err := s.topo.Exist(path.Join(s.topo.GetActionResponsePath(seq), s.info.Id))
		if err != nil {
			log.ErrorErrorf(err, "get action failed")
			//s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
			return err
		}
		if exist {
			continue
		}
		if s.checkAndDoTopoChange(seq) {
			s.responseAction(int64(seq))
		}
	}

	s.lastActionSeq = seqs[len(seqs)-1]
	return nil
}

func (s *Server) loopEvents() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var tick int = 0
	for s.info.State == models.PROXY_STATE_ONLINE {
		select {
		case <-s.kill:
			log.Infof("mark offline, proxy is killed: %s", s.info.Id)
			s.markOffline()
		case e := <-s.evtbus:
			evtPath := getEventPath(e)
			log.Infof("got event %s, %v, lastActionSeq %d", s.info.Id, e, s.lastActionSeq)
			if strings.Index(evtPath, models.GetActionResponsePath(s.conf.productName)) == 0 {
				seq, err := strconv.Atoi(path.Base(evtPath))
				if err != nil {
					log.ErrorErrorf(err, "parse action seq failed")
				} else {
					if seq < s.lastActionSeq {
						log.Infof("ignore seq = %d", seq)
						continue
					}
				}
			}
			err := s.processAction(e)
			if err != nil {
				s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
			}
		case <-ticker.C:
			if maxTick := s.conf.pingPeriod; maxTick != 0 {
				if tick++; tick >= maxTick {
					s.router.KeepAlive()
					tick = 0
				}
			}
		}
	}
}

func (s *Server) reRegister(state string) {
	if s.isStarting() {
		log.Infof("server is restarting")
		return
	}
	s.info.State = state
	s.topo.Close(s.info.Id)
	s.topo.InitZkConn()
	s.register()
	s.setServerStatus(SERVER_STATUS_STARTED)
}

func (s *Server) fillSlots() {
	for i := 0; i < router.MaxSlotNum; i++ {
		if err := s.fillSlot(i); err != nil {
			s.reRegisterAndFillSlots(models.PROXY_STATE_ONLINE)
			break
		}
	}
}

func (s *Server) reRegisterAndReWatchPrxoy(state string) {
	if s.isStarting() {
		log.Infof("server is restarting")
		return
	}
	log.Warnf("server will restart")
	s.setServerStatus(SERVER_STATUS_STARTING)
	s.info.State = state
	s.cleanup()
	s.topo.InitZkConn()
	s.register()
	s.topo.watchSuspend.Set(false)
	s.rewatchProxy()
	s.setServerStatus(SERVER_STATUS_STARTED)
}

func (s *Server) reRegisterAndFillSlots(state string) {
	if s.isStarting() {
		log.Warnf("server is restarting")
		return
	}
	log.Warnf("server will restart")
	s.setServerStatus(SERVER_STATUS_STARTING)
	s.info.State = state
	s.cleanup()
	s.topo.InitZkConn()
	s.register()
	s.rewatchProxy()
	s.rewatchNodes()
	s.topo.watchSuspend.Set(false)
	s.fillSlots()
	s.setServerStatus(SERVER_STATUS_STARTED)
	log.Warnf("server restarted")
}

func (s *Server) isStarting() bool {
	s.startLock.Lock()
	defer s.startLock.Unlock()
	return s.status == SERVER_STATUS_STARTING
}

func (s *Server) setServerStatus(status int) {
	s.startLock.Lock()
	defer s.startLock.Unlock()
	s.status = status
}

func (s *Server) cleanup() {
	s.topo.watchSuspend.CompareAndSwap(false, true)
	close(s.evtbus)
	s.topo.Close(s.info.Id)
	s.evtbus = make(chan interface{}, 1000)
}

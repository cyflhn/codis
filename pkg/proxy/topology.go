// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"encoding/json"
	"path"

	topo "github.com/wandoulabs/go-zookeeper/zk"

	"github.com/wandoulabs/codis/pkg/models"
	"github.com/wandoulabs/codis/pkg/utils/atomic2"
	"github.com/wandoulabs/codis/pkg/utils/errors"
	"github.com/wandoulabs/codis/pkg/utils/log"
	"github.com/wandoulabs/zkhelper"
	"time"
)

const (
	ZK_RECONNECT_INTERVAL = 5
)

type TopoUpdate interface {
	OnGroupChange(groupId int)
	OnSlotChange(slotId int)
}

//type ZkFactory func(zkAddr string, zkSessionTimeout int) (zkhelper.Conn, error)

type ZkFactory interface {
	Connect(Addr string, SessionTimeout int) (zkhelper.Conn, error)
	ConnectWithConf(Addr string, conf topo.ConnConf) (zkhelper.Conn, error)
}

type Topology struct {
	ProductName      string
	zkAddr           string
	zkConn           zkhelper.Conn
	fact             ZkFactory
	provider         string
	zkSessionTimeout int
	readTimeout      int
	connTimeout      int
	watchSuspend     *atomic2.Bool
}

func (top *Topology) GetGroup(groupId int) (*models.ServerGroup, error) {
	var group *models.ServerGroup
	var err error
	for {
		group, err = models.GetGroup(top.zkConn, top.ProductName, groupId)
		if err == nil || top.IsFatalErr(err) {
			break
		} else {
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		}
		if err != nil {
			log.ErrorErrorf(err, " GetGroup ")
		}
	}
	return group, err
}

func (top *Topology) Exist(path string) (bool, error) {
	var exist bool
	var err error
	for {
		exist, err = zkhelper.NodeExists(top.zkConn, path)
		if err == nil || top.IsFatalErr(err) {
			break
		} else {
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		}
		if err != nil {
			log.ErrorErrorf(err, " Exist ")
		}
	}
	return exist, err
}

func (top *Topology) GetSlotByIndex(i int) (*models.Slot, *models.ServerGroup, error) {
	var slot *models.Slot
	var groupServer *models.ServerGroup
	var err error
	for {
		slot, err = models.GetSlot(top.zkConn, top.ProductName, i)
		if err == nil || top.IsFatalErr(err) {
			break
		} else {
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		}
		if err != nil {
			log.ErrorErrorf(err, " GetSlot ")
		}
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for {
		groupServer, err = models.GetGroup(top.zkConn, top.ProductName, slot.GroupId)
		if err == nil || top.IsFatalErr(err) {
			break
		} else {
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		}
		if err != nil {
			log.ErrorErrorf(err, " GetGroup ")
		}
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return slot, groupServer, nil
}

func NewTopo(ProductName string, zkAddr string, f ZkFactory, provider string, zkSessionTimeout int) *Topology {
	return NewTopoWithNetConf(ProductName, zkAddr, f, provider, 10, 30, zkSessionTimeout)
}

func NewTopoWithNetConf(ProductName string, zkAddr string, f ZkFactory, provider string, connTimeout int, readTimeout int, zkSessionTimeout int) *Topology {
	t := &Topology{
		zkAddr:           zkAddr,
		ProductName:      ProductName,
		fact:             f,
		provider:         provider,
		zkSessionTimeout: zkSessionTimeout,
		readTimeout:      readTimeout,
		connTimeout:      connTimeout,
		watchSuspend:     &atomic2.Bool{},
	}
	t.watchSuspend.Set(false)
	if t.fact == nil {
		switch t.provider {
		case "etcd":
			t.fact = &zkhelper.EtcdConnector{}
		case "zookeeper":
			t.fact = &zkhelper.ZkConnector{}
		default:
			log.Panicf("coordinator not found in config")
		}
	}
	t.InitZkConn()
	return t
}

func (top *Topology) InitZkConn() {
	var err error
	for {
		connConf := topo.ConnConf{time.Duration(top.readTimeout) * time.Second, time.Duration(top.connTimeout) * time.Second, int32(top.zkSessionTimeout * 1000)}
		top.zkConn, err = top.fact.ConnectWithConf(top.zkAddr, connConf)
		if err != nil {
			log.ErrorErrorf(err, "init failed")
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}
}

func (top *Topology) GetActionWithSeq(seq int64) (*models.Action, error) {
	var err error
	var act *models.Action
	for {
		act, err = models.GetActionWithSeq(top.zkConn, top.ProductName, seq, top.provider)
		if err == nil || top.IsFatalErr(err) {
			break
		} else {
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		}
		if err != nil {
			log.ErrorErrorf(err, " GetActionWithSeq ")
			act = nil
		}
	}
	return act, err
}

func (top *Topology) GetActionWithSeqObject(seq int64, act *models.Action) error {
	var err error
	for {
		err = models.GetActionObject(top.zkConn, top.ProductName, seq, act, top.provider)
		if err == nil || top.IsFatalErr(err) {
			break
		} else {
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		}
		if err != nil {
			log.ErrorErrorf(err, " GetActionWithSeqObject ")
		}
	}
	return err
}

func (top *Topology) GetActionSeqList(productName string) ([]int, error) {
	return models.GetActionSeqList(top.zkConn, productName)
}

func (top *Topology) IsChildrenChangedEvent(e interface{}) bool {
	return e.(topo.Event).Type == topo.EventNodeChildrenChanged
}

func (top *Topology) CreateProxyInfo(pi *models.ProxyInfo) (string, error) {
	return models.CreateProxyInfo(top.zkConn, top.ProductName, pi)
}

func (top *Topology) CreateProxyFenceNode(pi *models.ProxyInfo) (string, error) {
	return models.CreateProxyFenceNode(top.zkConn, top.ProductName, pi)
}

func (top *Topology) GetProxyInfo(proxyName string) (*models.ProxyInfo, error) {
	var err error
	var proxy *models.ProxyInfo
	for {
		proxy, err = models.GetProxyInfo(top.zkConn, top.ProductName, proxyName)
		if err == nil || top.IsFatalErr(err) {
			break
		} else {
			time.Sleep(ZK_RECONNECT_INTERVAL * time.Second)
		}
		if err != nil {
			log.ErrorErrorf(err, "GetProxyInfo ")
			proxy = nil
		}
	}
	return proxy, err
}

func (top *Topology) GetActionResponsePath(seq int) string {
	return path.Join(models.GetActionResponsePath(top.ProductName), top.zkConn.Seq2Str(int64(seq)))
}

func (top *Topology) SetProxyStatus(proxyName string, status string) error {
	return models.SetProxyStatus(top.zkConn, top.ProductName, proxyName, status)
}

func (top *Topology) Close(proxyName string) {
	// delete fence znode
	pi, err := models.GetProxyInfo(top.zkConn, top.ProductName, proxyName)
	if err != nil {
		log.Errorf("killing fence error, proxy %s is not exists", proxyName)
	}
	if pi != nil {
		zkhelper.DeleteRecursive(top.zkConn, path.Join(models.GetProxyFencePath(top.ProductName), pi.Addr), -1)
	}

	// delete ephemeral znode
	zkhelper.DeleteRecursive(top.zkConn, path.Join(models.GetProxyPath(top.ProductName), proxyName), -1)
	top.zkConn.Close()
}

func (top *Topology) DoResponse(seq int, pi *models.ProxyInfo) error {
	//create response node
	actionPath := top.GetActionResponsePath(seq)
	//log.Debug("actionPath:", actionPath)
	data, err := json.Marshal(pi)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = top.zkConn.Create(path.Join(actionPath, pi.Id), data,
		0, zkhelper.DefaultFileACLs())

	return err
}

func (top *Topology) doWatch(evtch <-chan topo.Event, evtbus chan interface{}) {
	e := <-evtch
	log.Warnf("topo event %+v", e)
	switch e.Type {
	//case topo.EventNodeCreated:
	//case topo.EventNodeDataChanged:
	case topo.EventNodeChildrenChanged: //only care children changed
		//todo:get changed node and decode event
	default:
		//log.Warnf("%+v", e)
	}
	if top.watchSuspend.Get() == false {
		evtbus <- e
	}
}

func (top *Topology) WatchChildren(path string, evtbus chan interface{}) ([]string, error) {
	content, _, evtch, err := top.zkConn.ChildrenW(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go top.doWatch(evtch, evtbus)
	return content, nil
}

func (top *Topology) WatchNode(path string, evtbus chan interface{}) ([]byte, error) {
	content, _, evtch, err := top.zkConn.GetW(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go top.doWatch(evtch, evtbus)
	return content, nil
}

func (top *Topology) RefreshZkConn() {
	top.zkConn.Close()
	top.InitZkConn()
}

func (top *Topology) IsFatalErr(err error) bool {
	return zkhelper.ZkErrorEqual(err, topo.ErrSessionExpired) || zkhelper.ZkErrorEqual(err, topo.ErrNoNode) || top.IsErrClosing(err)
}

func (top *Topology) IsErrSessionExpired(err error) bool {
	return zkhelper.ZkErrorEqual(err, topo.ErrSessionExpired)
}

func (top *Topology) IsErrNoNode(err error) bool {
	return zkhelper.ZkErrorEqual(err, topo.ErrNoNode)
}

func (top *Topology) IsErrClose(err error) bool {
	return zkhelper.ZkErrorEqual(err, topo.ErrClosing) || zkhelper.ZkErrorEqual(err, topo.ErrConnectionClosed) || zkhelper.ZkErrorEqual(err, topo.ErrNoServer)
}

func (top *Topology) IsErrClosing(err error) bool {
	return zkhelper.ZkErrorEqual(err, topo.ErrClosing)
}

func (top *Topology) IsSessionExpiredEvent(e interface{}) bool {
	return e.(topo.Event).State == topo.StateExpired || e.(topo.Event).Type == topo.EventNotWatching
}

func (top *Topology) IsNodeDeletedEvent(e interface{}) bool {
	return e.(topo.Event).Type == topo.EventNodeDeleted
}

func (top *Topology) IsErrNodeExist(err error) bool {
	return zkhelper.ZkErrorEqual(err, topo.ErrNodeExists)
}

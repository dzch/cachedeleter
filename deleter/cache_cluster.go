package deleter

import (
		"github.com/dzch/go-utils/logger"
		"errors"
		"fmt"
		"time"
		"sort"
	   )

type CacheCluster struct {
	clusterName string
	ccc *CacheClusterConf
	cachePool CachePool
	mqClient *MqClient
}

func (cc *CacheCluster) init() (err error) {
	err = cc.initCachePool()
	if err != nil {
		return
	}
	err = cc.initMqClient()
	if err != nil {
		return
	}
	logger.Notice("success init CacheCluster: [%s]", cc.clusterName)
	return nil
}

func (cc *CacheCluster) initCachePool() (err error) {
	/* TODO: 目前只支持redis作为cache */
	if !cc.ccc.isRedis {
		err = errors.New(fmt.Sprintf("Redis cache required! fail to initCachePool for cluster [%s]", cc.clusterName))
		return
	}
    cc.cachePool = &RedisCachePool {
        clusterName: cc.clusterName,
		ccc: cc.ccc,
	}
	return cc.cachePool.init()
}

func (cc *CacheCluster) initMqClient() (err error) {
	cc.mqClient = &MqClient {
        clusterName: cc.clusterName,
		delMqServerAddrs: cc.ccc.delMqServers,
		idcMqServerAddrs: cc.ccc.idcMqServers,
		mqTimeout: cc.ccc.mqTimeout,
	}
	return cc.mqClient.init()
}

func (cc *CacheCluster) doDel(keys []interface{}, delTime, curDelay int64, doneServers map[string]interface{}, isFromMq, firstDel bool) (err error) {
	if isFromMq {
		return cc.doDelFromMq(keys, delTime, curDelay, doneServers, firstDel)
	}
	return cc.mqClient.addKeys(keys, doneServers, delTime, int64(cc.ccc.delayConfig[0]), true)
}

func (cc *CacheCluster) doDelFromMq(keys []interface{}, delTime, curDelay int64, doneServers map[string]interface{}, firstDel bool) (err error) {
	/* time to del ? */
    now := time.Now().Unix()
	delta := delTime+curDelay - now
	logger.Debug("delta=%d, delTime=%d, curDelay=%d, now=%d, firtDel=%t", delta, delTime, curDelay, now, firstDel)
	if delta > 0 {
		time.Sleep(time.Duration(delta)*time.Second)
		now = now+delta
	}
	err = cc.cachePool.doDel(keys, doneServers)
	if err != nil {
		/* put back to mq */
	    err = cc.mqClient.addKeys(keys, doneServers, delTime, curDelay, false)
		if err != nil {
			return
		}
	} else {
	    logger.Notice("del all keys done: cluster=%s, keys=%v, delay=%d", cc.clusterName, keys, curDelay)
	}
	if !firstDel {
		/* not first Del, so all delays has been in mq, we just quit and wait */
		return nil
	}
	/* ok, it is first del, we add all delays to mq here */
    ccc := cc.ccc
	delay := now - delTime
	logger.Debug("%d = %d - %d", delay, now, delTime)
    idx := sort.SearchInts(ccc.delayConfig, int(delay))
	if idx == len(ccc.delayConfig) {
		/* done */
		return nil
	}
	for ; idx < len(ccc.delayConfig); idx ++ {
	    if int64(ccc.delayConfig[idx]) <= delay {
			continue
		}
	    // add back to mq with doneServers empty
	    err = cc.mqClient.addKeys(keys, nil, delTime, int64(ccc.delayConfig[idx]), false)
		if err != nil {
			return
		}
	}
	return nil
}


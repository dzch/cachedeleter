/*
    The MIT License (MIT)
    
	Copyright (c) 2015 myhug.cn and zhouwench (zhouwench@gmail.com)
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/
package deleter

import (
		"github.com/dzch/go-utils/logger"
		"github.com/garyburd/redigo/redis"
		"time"
		"errors"
		"fmt"
	   )

var (
		gRedisPoolIdleTimeout = 240*time.Second
	)

type RedisCachePool struct {
	clusterName string
	ccc *CacheClusterConf
	redisServerPool map[string]*redis.Pool
}

type RedisDialer struct {
	addr string
	connTimeout time.Duration
	readTimeout time.Duration
	writeTimeout time.Duration
}

func (rd *RedisDialer) dial() (redis.Conn, error) {
	return redis.DialTimeout("tcp", rd.addr, rd.connTimeout, rd.readTimeout, rd.writeTimeout)
}

func (rd *RedisDialer) testOnBorrow(c redis.Conn, t time.Time) error {
	_, err := c.Do("PING")
	return err
}

func (rcp *RedisCachePool) init() (err error) {
	err = rcp.initRedisServers()
	if err != nil {
		return
	}
	logger.Notice("success init RedisPools for cluster [%s]", rcp.clusterName)
	return nil
}

func (rcp *RedisCachePool) initRedisServers() (err error) {
	rcp.redisServerPool = make(map[string]*redis.Pool, len(rcp.ccc.serverAddrs))
    ccc := rcp.ccc
	for _, addr := range ccc.serverAddrs {
        rd := &RedisDialer {
            addr: addr,
			connTimeout: ccc.connTimeout,
			readTimeout: ccc.readTimeout,
			writeTimeout: ccc.writeTimeout,
		}
        rp := &redis.Pool {
            MaxIdle: ccc.minConnsEach,
			MaxActive: ccc.maxConnsEach,
			IdleTimeout: gRedisPoolIdleTimeout,
			Dial: rd.dial,
			TestOnBorrow: rd.testOnBorrow,
		}
		rcp.redisServerPool[addr] = rp
	}
	return nil
}

func (rcp *RedisCachePool) doDel(keys []interface{}, doneServers map[string]interface{}) (err error) {
    toc := make(chan bool, 1)
	go rcp.delAllTimeout(toc)
	hasError := false
	needReceiveConns := make(map[string]redis.Conn, len(rcp.redisServerPool))
	var firstErr error
	for addr, pool := range rcp.redisServerPool {
		if _, ok := doneServers[addr]; ok {
			continue
		}
        conn := pool.Get()
		if err = conn.Err(); err != nil {
			hasError = true
			if firstErr == nil {
				firstErr = err
			}
			logger.Warning("fail to get conn for server: %s, %s", addr, err.Error())
			continue
		}
		defer conn.Close()
		err = conn.Send("del", keys...)
		if err != nil {
			hasError = true
			if firstErr == nil {
				firstErr = err
			}
			logger.Warning("fail to conn.Send for server: %s, %s", addr, err.Error())
			continue
		}
		err = conn.Flush()
		if err != nil {
			hasError = true
			if firstErr == nil {
				firstErr = err
			}
			logger.Warning("fail to conn.Flush for server: %s, %s", addr, err.Error())
			continue
		}
		needReceiveConns[addr] = conn
	}
	select {
		case <-toc:
			hasError = true
			if firstErr == nil {
				firstErr = err
			}
			err = errors.New(fmt.Sprintf("del All timeout for cluster: %s", rcp.clusterName))
			goto errout
		default:
			/* do nothine */
	}
//	logger.Debug("need receive servers: %d", len(needReceiveConns))
	for addr, conn := range needReceiveConns {
		_, err = conn.Receive()
		if err != nil {
			hasError = true
			if firstErr == nil {
				firstErr = err
			}
			logger.Warning("fail to conn.Receive: %s", err.Error())
			continue
		} else {
			doneServers[addr] = true
		}
	    select {
		    case <-toc:
			    hasError = true
			    if firstErr == nil {
				    firstErr = err
			    }
			    err = errors.New(fmt.Sprintf("del All timeout for cluster: %s", rcp.clusterName))
			    goto errout
		    default:
			    /* do nothine */
	    }
	}
	if hasError {
		goto errout
	}
	return nil

errout:
    err = firstErr
    logger.Warning("%s", err.Error())
	return
}

func (rcp *RedisCachePool) delAllTimeout(toc chan bool) {
	<-time.After(rcp.ccc.delAllTimeout)
	toc <-true
}

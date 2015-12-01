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
		"github.com/go-yaml/yaml"
		"github.com/tinylib/msgp/msgp"
		"time"
		"runtime"
		"io/ioutil"
		"errors"
		"net/http"
		"fmt"
		"strings"
		"sort"
		"io"
		"bytes"
	   )

type Deleter struct {
	confFile string
	logDir string
	logLevel int
	clusterConfFile string
	listenPort uint16
	cacheClusterConfs map[string]*CacheClusterConf
	cacheClusters map[string]*CacheCluster
	httpServer *http.Server
	goMaxProcs int
}

type CacheClusterConf struct {
	clusterName string
	isRedis bool
	delayConfig []int
	delMqServers []string
	idcMqServers []string
	serverAddrs []string
	minConnsEach int
	maxConnsEach int
	delAllTimeout time.Duration
	mqTimeout time.Duration
	connTimeout time.Duration
	readTimeout time.Duration
	writeTimeout time.Duration
}

func NewDeleter(confFile string) (deleter *Deleter, err error) {
	deleter = &Deleter {
        confFile: confFile,
	}
	return deleter, deleter.init()
}

func (d *Deleter) init() (err error) {
	err = d.initConfig()
	if err != nil {
		return
	}
	err = d.initRuntimeConf()
	if err != nil {
		return
	}
	err = d.initLog()
	if err != nil {
		return
	}
	err = d.initCacheClusters()
	if err != nil {
		return
	}
	err = d.initServer()
	if err != nil {
		return
	}
	logger.Notice("deleter init success")
	return nil
}

func (d *Deleter) Run() (err error) {
    err = d.httpServer.ListenAndServe()
	if err != nil {
		logger.Warning("fail to httpServer.ListenAndServe: %s", err.Error())
		return
	}
	logger.Notice("httpServer.ListenAndServe end")
	return nil
}

func (d *Deleter) initConfig() (err error) {
    content, err := ioutil.ReadFile(d.confFile)
	if err != nil {
		return
	}
    m := make(map[interface{}]interface{})
	err = yaml.Unmarshal(content, &m)
	if err != nil {
		return
	}

	/* log conf */
    logDir, ok := m["log_dir"]
	if !ok {
		return errors.New("log_dir not found in conf file")
    }
	d.logDir = logDir.(string)
	logLevel, ok := m["log_level"]
	if !ok {
		return errors.New("log_level not found in conf file")
	}
	d.logLevel = logLevel.(int)

	/* listen port */
	port, ok := m["port"]
	if !ok {
		return errors.New("port not found in conf file")
	}
	d.listenPort = uint16(port.(int))

	/* go runtime */
	maxProcs, ok := m["go_max_procs"]
	if !ok {
		d.goMaxProcs = runtime.NumCPU()
	} else {
		d.goMaxProcs = int(maxProcs.(int))
	}

	/* cluster conf */
	/* TODO: 这里只是想用proxy的server配置，所以cluster配置会有点奇怪 */
	ccf, ok := m["cache_server_conf"]
	if !ok {
		return errors.New("cache_server_conf not found in conf file")
	}
	d.clusterConfFile = ccf.(string)
	clusters, ok := m["cluster_config"]
	if !ok {
		return errors.New("cluster_config not found in conf file")
	}
	d.cacheClusterConfs = make(map[string]*CacheClusterConf)
	for name, clusterConf := range clusters.(map[interface{}]interface{}) {
        ccc := &CacheClusterConf{}
		ccc.clusterName = name.(string)
        cluster := clusterConf.(map[interface{}]interface{})
		/* delays */
		delayConfigs, ok := cluster["delays"]
		if !ok {
			return errors.New(fmt.Sprintf("%s.delays not found in conf file", name.(string)))
		}
		for _, delay := range delayConfigs.([]interface{}) {
			ccc.delayConfig = append(ccc.delayConfig, delay.(int))
		}
        dc := sort.IntSlice(ccc.delayConfig)
		dc.Sort()
		if dc[0] <= 0 {
			return errors.New(fmt.Sprintf("%s.delays cannot <= 0 in conf file", name.(string)))
		}
		ccc.delayConfig = dc
		/* mq servers */
		mqs, ok := cluster["del_mqs"]
		if !ok {
			return errors.New(fmt.Sprintf("%s.del_mqs not found in conf file", name.(string)))
		}
		for _, mq := range mqs.([]interface{}) {
			ccc.delMqServers = append(ccc.delMqServers, mq.(string))
		}
		mqs, ok = cluster["idc_mqs"]
		if ok {
		    for _, mq := range mqs.([]interface{}) {
			    ccc.idcMqServers = append(ccc.idcMqServers, mq.(string))
		    }
		}
		/* min conns */
		minConns, ok := cluster["min_conns_each_server"]
		if !ok {
			return errors.New(fmt.Sprintf("%s.min_conns_each_server not found in conf file", name.(string)))
		}
		ccc.minConnsEach = minConns.(int)
		/* max conns */
		maxConns, ok := cluster["max_conns_each_server"]
		if !ok {
			return errors.New(fmt.Sprintf("%s.max_conns_each_server not found in conf file", name.(string)))
		}
		ccc.maxConnsEach = maxConns.(int)
		/* timeout */
	    connTimeout, ok := cluster["conn_timeout_ms"]
	    if !ok {
		    return errors.New(fmt.Sprintf("%s.conn_timeout_ms not found in conf file", name))
	    }
	    ccc.connTimeout = time.Duration(connTimeout.(int))*time.Millisecond
	    readTimeout, ok := cluster["read_timeout_ms"]
	    if !ok {
		    return errors.New(fmt.Sprintf("%s.read_timeout_ms not found in conf file", name))
	    }
	    ccc.readTimeout = time.Duration(readTimeout.(int))*time.Millisecond
	    writeTimeout, ok := cluster["write_timeout_ms"]
	    if !ok {
		    return errors.New(fmt.Sprintf("%s.write_timeout_ms not found in conf file", name))
	    }
	    ccc.writeTimeout = time.Duration(writeTimeout.(int))*time.Millisecond
	    delAllTimeout, ok := cluster["del_all_timeout_ms"]
	    if !ok {
	        return errors.New(fmt.Sprintf("%s.del_all_timeout_ms not found in conf file", name))
	    }
	    ccc.delAllTimeout = time.Duration(delAllTimeout.(int))*time.Millisecond
	    mqTimeout, ok := cluster["mq_timeout_ms"]
	    if !ok {
	        return errors.New(fmt.Sprintf("%s.mq_timeout_ms not found in conf file", name))
	    }
	    ccc.mqTimeout = time.Duration(mqTimeout.(int))*time.Millisecond
	    /* final */
	    d.cacheClusterConfs[name.(string)] = ccc
	}
	return nil
}

func (d *Deleter) initRuntimeConf() (err error) {
	runtime.GOMAXPROCS(d.goMaxProcs)
	return
}

func (d *Deleter) initLog() (err error) {
	err = logger.Init(d.logDir, "cachedeleter", logger.LogLevel(d.logLevel))
	return
}

func (d *Deleter) initCacheClusters() (err error) {
    content, err := ioutil.ReadFile(d.clusterConfFile)
	if err != nil {
		return
	}
    m := make(map[interface{}]interface{})
	err = yaml.Unmarshal(content, &m)
	if err != nil {
		return
	}

	d.cacheClusters = make(map[string]*CacheCluster)
	for name, ccc := range d.cacheClusterConfs {
		clusterConf, ok := m[name]
		if !ok {
			err = errors.New(fmt.Sprintf("cluster [%s] not found in %s", name, d.clusterConfFile))
			logger.Warning("%s", err.Error())
			return
		}
        cluster := clusterConf.(map[interface{}]interface{})
		redis, ok := cluster["redis"]
		isRedis := false
		if ok && redis.(bool) {
			isRedis = true
		}
        servers, ok := cluster["servers"]
		if !ok {
			err = errors.New(fmt.Sprintf("servers not found in cluster [%s], in %s", name, d.clusterConfFile))
			logger.Warning("%s", err.Error())
			return
		}
		if len(servers.([]interface{})) == 0 {
			err = errors.New(fmt.Sprintf("num of servers is zero in cluster [%s], in %s", name, d.clusterConfFile))
			logger.Warning("%s", err.Error())
			return
		}
		var addrs []string
		for _, server := range servers.([]interface{}) {
            sa := strings.Split(server.(string), ":")
			if sa[0][0] == 'm' || sa[0][0] == 's' {
				err = errors.New(fmt.Sprintf("servers is not in cache mode, cluster [%s]", name))
				logger.Warning("%s", err.Error())
				return
			}
			addrs = append(addrs, fmt.Sprintf("%s:%s", sa[0], sa[1]))
		}
		ccc.isRedis = isRedis
		ccc.serverAddrs = addrs
        cc := &CacheCluster {
            clusterName: name,
            ccc: ccc,
		}
		err = cc.init()
		if err != nil {
			logger.Warning("fail to init CacheCluster for [%s]", name)
			return
		}
		d.cacheClusters[name] = cc
	}

	return nil
}

func (d *Deleter) initServer() (err error) {
    mux := http.NewServeMux()
	mux.HandleFunc("/del", d.doDel)
	d.httpServer = &http.Server {
        Addr: fmt.Sprintf(":%d", d.listenPort),
		Handler: mux,
	}
	return nil
}

func (d *Deleter) doDel(w http.ResponseWriter, r *http.Request) {
	/* get topic and method */
	logger.Debug("get one req: %s", r.URL.String())
    qv := r.URL.Query()
	topic := qv.Get("topic")
	if len(topic) == 0 {
        msg := fmt.Sprintf("invalid query, topic cannot be empty: %s", r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	cc, ok := d.cacheClusters[topic]
	if !ok {
        msg := fmt.Sprintf("invalid query, cache cluster is not exist: cluster [%s], %s", topic, r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}

	/* get post data */
	if r.ContentLength == 0 {
		logger.Warning("post data cannot be empty")
		d.response(w, http.StatusBadRequest, "post data cannot be empty")
		return
	}
    data := make([]byte, r.ContentLength)
	_, err := io.ReadFull(r.Body, data)
	if err != nil {
        msg := fmt.Sprintf("fail to read post data: %s", err.Error())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	/* unpack */
	buf := bytes.NewReader(data)
	buf.Seek(0, 0)
	msgr := msgp.NewReader(buf)
	reqi, err := msgr.ReadIntf()
	if err != nil {
        msg := fmt.Sprintf("fail to decode post data: %s", err.Error())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	req, ok := reqi.(map[string]interface{})
	if !ok {
        msg := fmt.Sprintf("invalid post data: data need map", r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	keysi, ok := req[gDelKeys]
	if !ok {
        msg := fmt.Sprintf("invalid post data: %s not exist: %s", gDelKeys, r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	keys, ok := keysi.([]interface{})
	if !ok {
        msg := fmt.Sprintf("invalid post data: %s not array: %s", gDelKeys, r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	if len(keys) == 0 {
        msg := fmt.Sprintf("invalid post data: len of %s array is 0: %s", gDelKeys, r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	delTimei, ok := req[gDelDelTime]
	if !ok {
        msg := fmt.Sprintf("invalid post data: %s not exist: %s", gDelDelTime, r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	delTime, ok := delTimei.(int64)
	if !ok {
	    delTimet, ok := delTimei.(int)
		if !ok {
			delTimetu, ok := delTimei.(uint64)
		    if !ok {
                msg := fmt.Sprintf("invalid post data: %s not int64 or uint64 or int: %s", gDelDelTime, r.URL.String())
		        logger.Warning("%s", msg)
		        d.response(w, http.StatusBadRequest, msg)
		        return
			} else {
				delTime = int64(delTimetu)
			}
		} else {
		    delTime = int64(delTimet)
		}
	}
	var curDelay int64
	curDelayi, ok := req[gDelCurDelay]
	if !ok {
		curDelay = 0
	} else if curDelay, ok = curDelayi.(int64); !ok {
		curDelayt, ok := curDelayi.(int)
		if !ok {
			curDelaytu, ok := curDelayi.(uint64)
			if !ok {
                msg := fmt.Sprintf("invalid post data: %s not int64 or int: %s", gDelCurDelay, r.URL.String())
		        logger.Warning("%s", msg)
		        d.response(w, http.StatusBadRequest, msg)
		        return
			} else {
				curDelay = int64(curDelaytu)
			}
		} else {
		    curDelay = int64(curDelayt)
		}
	}
	var isFromMq bool
	isFromMqi, ok := req[gDelFromMq]
	if !ok {
		isFromMq = false
	} else if isFromMq, ok = isFromMqi.(bool); !ok {
        msg := fmt.Sprintf("invalid post data: %s not bool: %s", gDelFromMq, r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	var doneServers map[string]interface{}
	doneServersi, ok := req[gDoneServers]
	if !ok {
		doneServers = make(map[string]interface{})
	} else if doneServers, ok = doneServersi.(map[string]interface{}); !ok {
        msg := fmt.Sprintf("invalid post data: %s not map[string]interface{}: %s", gDoneServers, r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}
	var firstDel bool
	firstDeli, ok := req[gFirstDel]
	if !ok {
		firstDel = true
	} else if firstDel, ok = firstDeli.(bool); !ok {
        msg := fmt.Sprintf("invalid post data: %s not bool: %s", gFirstDel, r.URL.String())
		logger.Warning("%s", msg)
		d.response(w, http.StatusBadRequest, msg)
		return
	}

	/* do del */
	err = cc.doDel(keys, delTime, curDelay, doneServers, isFromMq, firstDel)
	if err != nil {
        msg := fmt.Sprintf("fail to del: %s", err.Error())
		logger.Warning("%s", msg)
		d.response(w, http.StatusInternalServerError, msg)
		return
	}
    msg := fmt.Sprintf("success process: %s", r.URL.String())
	logger.Notice("%s", msg)
	d.response(w, http.StatusOK, msg)
	return
}

func (d *Deleter) response(w http.ResponseWriter, statusCode int, errMsg string) {
	var buf bytes.Buffer
	buf.Reset()
	res := map[string]interface{} {
		"error": map[string]interface{} {
			"errno": statusCode,
				/* some msgpack lib cannot support new encoding-spec， so let len(string) < 31 */
//			"errmsg": "success process: /del?topic=ap_",
			"errmsg": "see errno please",
		},
	}
    wr := msgp.NewWriter(&buf)
	err := wr.WriteIntf(res)
	if err != nil {
		logger.Warning("fail to msgp.wr.WriteIntf: %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	wr.Flush()
	w.WriteHeader(statusCode)
	_, err = w.Write(buf.Bytes())
	if err != nil {
		logger.Warning("fail to http.ResponseWriter.Write(): %s", err.Error())
		return
	}
//	/* unpack test */
//	b := bytes.NewReader(buf.Bytes())
//	b.Seek(0, 0)
//	msgr := msgp.NewReader(b)
//	reqi, err := msgr.ReadIntf()
//	if err != nil {
//        msg := fmt.Sprintf("fail to decode post data: %s", err.Error())
//		logger.Warning("%s", msg)
//		return
//	}
//	logger.Warning("unpack: %v", reqi)
}


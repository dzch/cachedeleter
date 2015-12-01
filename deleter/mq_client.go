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
		"github.com/tinylib/msgp/msgp"
		"github.com/dzch/go-utils/logger"
		"net/http"
		"time"
		"bytes"
		"fmt"
		"errors"
	   )

var (
		gDelMethodPrefix = "delay_"
		gDelKeys = "keys"
		gDelDelTime = "del_time"
		gDelCurDelay = "cur_delay"
		gDelFromMq = "from_mq"
		gDoneServers = "done_servers"
		gFirstDel = "first_del"
	)

type MqClient struct {
	clusterName string
	delMqServerAddrs []string
	idcMqServerAddrs []string
	mqTimeout time.Duration
	lastServerId int
	client *http.Client
}

func (mc *MqClient) init() (err error) {
	mc.lastServerId = -1
	mc.client = &http.Client {
        Timeout: mc.mqTimeout,
	}
	return nil
}

func (mc *MqClient) addKeys(keys []interface{}, doneServers map[string]interface{}, delTime, nextDelay int64, firstDel bool) (err error) {
	/* pack data */
    data := map[string]interface{} {
        gDelKeys: keys,
		gDelDelTime: delTime,
		gDelCurDelay: nextDelay,
		gDelFromMq: true,
		gFirstDel: firstDel,
		"method": fmt.Sprintf("%s%d",gDelMethodPrefix, nextDelay), // ktransfer need it
	}
	if len(doneServers) > 0 {
		data[gDoneServers] = doneServers
	}
	var buf bytes.Buffer
	wr := msgp.NewWriter(&buf)
	err = wr.WriteIntf(data)
	if err != nil {
		logger.Warning("fail to msgp.WriteIntf: %s", err.Error())
		return
	}
	wr.Flush()
	r := bytes.NewReader(buf.Bytes())
	/* write mq */
	var mqServerAddrs []string
	if firstDel && len(mc.idcMqServerAddrs) > 0 {
		mqServerAddrs = mc.idcMqServerAddrs
	} else {
		mqServerAddrs = mc.delMqServerAddrs
	}
    toc := make(chan bool, 1)
	go mc.checkMqTimeout(toc)
	ns := len(mqServerAddrs)
	mc.lastServerId = (mc.lastServerId+1)%ns
	i := 0
	for {
		r.Seek(0, 0)
        url := fmt.Sprintf("http://%s?topic=%s&method=%s%d", mqServerAddrs[mc.lastServerId], mc.clusterName, gDelMethodPrefix, nextDelay)
	    req, err1 := http.NewRequest("POST", url, r)
		if err1 != nil {
			logger.Warning("fail to http.NewRequest: %s, %s", url, err1.Error())
			err = err1
			return
		}
		rsp, err1 := mc.client.Do(req)
		if err1 != nil {
			logger.Warning("fail to http.Client.Do: %s, %s", url, err1.Error())
			select {
				case <-toc:
					err = errors.New("add back to mq timeout")
					return
				default:
					/* do nothing */
			}
		    i ++
		    if i == ns {
				err = errors.New("fail to add back to mq, all servers have been retried")
				return
			}
	        mc.lastServerId = (mc.lastServerId+1)%ns
			continue
		} else {
			rsp.Body.Close()
			logger.Notice("success add back to mq: %s", url)
			return nil
		}
	}
}

func (mc *MqClient) checkMqTimeout(toc chan bool) {
	<-time.After(mc.mqTimeout)
	toc<-true
}

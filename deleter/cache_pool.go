package deleter

type CachePool interface {
	init() (err error)
	doDel(keys []interface{}, doneServers map[string]interface{}) (err error)
}

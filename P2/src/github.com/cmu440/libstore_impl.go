package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"
	// "fmt"
)

const MAX = 1024

type NodeIDArray []uint32

func (arr NodeIDArray) Len() int {
	return len(arr)
}
func (arr NodeIDArray) Less(i, j int) bool {
	return arr[i] < arr[j]
}
func (arr NodeIDArray) Swap(i, j int) {
	arr[i], arr[j] = arr[j], arr[i]
}

type libstore struct {
	master        *rpc.Client
	hostPort      string
	mode          LeaseMode
	slave         map[uint32]*rpc.Client
	nodeIDs       NodeIDArray
	cache         map[string]*cacheLease
	mutex         sync.Mutex
	ticker        *time.Ticker
	tickerCount   *time.Ticker
	cacheCount    map[string]int
	cacheTime     map[string]int
	mutexTime     sync.Mutex
	mutexAll      sync.Mutex
}

type cacheLease struct {
	key     string
	value   interface{}
	seconds int
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage servern (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	libstore := &libstore{
		hostPort:      myHostPort,
		ticker:        time.NewTicker(time.Millisecond * 1000),
		mode:          mode,
		nodeIDs:       NodeIDArray{},
		slave:         make(map[uint32]*rpc.Client, 1),
		cache:         make(map[string]*cacheLease, 1),
		tickerCount:   time.NewTicker(time.Millisecond * 1000),
		cacheCount:    make(map[string]int, 1),
		cacheTime:     make(map[string]int, 1),
	}
	if err := rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore)); err != nil {
		return nil, err
	}
	var err error
	if libstore.master, err = rpc.DialHTTP("tcp", masterServerHostPort); err != nil {
		return nil, err
	}
	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	for i := 0; i <= 5; i++ {
		if err := libstore.master.Call("StorageServer.GetServers", args, &reply); err == nil {
			if reply.Status == storagerpc.OK {
				break
			}
		}
		time.Sleep(time.Millisecond * 1000)
	}
	if reply.Status != storagerpc.OK {
		return nil, errors.New("Error")
	}
	for _, server := range reply.Servers {
		slaveServer, err := rpc.DialHTTP("tcp", server.HostPort)
		if err != nil {
			return nil, err
		}
		libstore.slave[server.NodeID] = slaveServer
		libstore.nodeIDs = append(libstore.nodeIDs, server.NodeID)
	}
	sort.Sort(libstore.nodeIDs)
	go libstore.runTime()
	go libstore.runTimeCount()
	return libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	if lease, ok := ls.cache[key]; ok {
		value := lease.value.(string)
		return value, nil
	}
	wantLease := false
	if ls.mode == Always {
		wantLease = true
	} else if ls.mode == Normal {
		ls.mutexTime.Lock()
		if value, ok := ls.cacheCount[key]; ok {
			ls.cacheCount[key] = value + 1
		} else {
			ls.cacheCount[key] = 1
			ls.cacheTime[key] = 0
		}
		count := ls.cacheCount[key]
		ls.mutexTime.Unlock()
		if count >= storagerpc.QueryCacheThresh {
			wantLease = true
		}
	}
	args := &storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.hostPort}
	var reply storagerpc.GetReply
	server := ls.requestRouting(key)
	if err := server.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}
	if reply.Status != storagerpc.OK {
		return "", errors.New("Error Get")
	}
	if reply.Lease.Granted && reply.Lease.ValidSeconds > 0 {
		lease := &cacheLease{
			key:     key,
			value:   reply.Value,
			seconds: reply.Lease.ValidSeconds,
		}
		ls.cache[key] = lease
	}
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	// fmt.Println("enter the lib post")
	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	server := ls.requestRouting(key)
	if err := server.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New("Error Put")
	}
	// fmt.Println("enter the lib post")
	return nil
}

func (ls *libstore) Delete(key string) error {
	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	server := ls.requestRouting(key)
	if err := server.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.KeyNotFound {
		return errors.New("Key Not Found")
	}
	if reply.Status != storagerpc.OK {
		return errors.New("Error Put")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	if lease, ok := ls.cache[key]; ok {
		value := lease.value.([]string)
		return value, nil
	}
	wantLease := false
	if ls.mode == Always {
		wantLease = true
	} else if ls.mode == Normal {
		ls.mutexTime.Lock()
		if _, ok := ls.cacheCount[key]; ok {
			// ls.cacheCount[key] = value + 1
			ls.cacheCount[key]++
		} else {
			ls.cacheCount[key] = 1
			ls.cacheTime[key] = 0
		}
		count := ls.cacheCount[key]
		ls.mutexTime.Unlock()
		if count >= storagerpc.QueryCacheThresh {
			wantLease = true
		}
	}
	args := &storagerpc.GetArgs{Key: key, WantLease: wantLease, HostPort: ls.hostPort}
	var reply storagerpc.GetListReply
	server := ls.requestRouting(key)
	if err := server.Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	}
	if reply.Status != storagerpc.OK {
		return nil, errors.New("Error Get")
	}
	if reply.Lease.Granted && reply.Lease.ValidSeconds > 0 {
		lease := &cacheLease{
			key:     key,
			value:   reply.Value,
			seconds: reply.Lease.ValidSeconds,
		}
		ls.cache[key] = lease
	}
	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	server := ls.requestRouting(key)
	if err := server.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.ItemNotFound {
		return errors.New("Item Not Found")
	}
	if reply.Status == storagerpc.KeyNotFound {
		return errors.New("Key Not Found")
	}
	if reply.Status != storagerpc.OK {
		return errors.New("Error RemoveFromList")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	server := ls.requestRouting(key)
	if err := server.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}
	if reply.Status == storagerpc.ItemExists {
		return errors.New("Item Exists")
	}
	if reply.Status != storagerpc.OK {
		return errors.New("Error AppendToList")
	}
	return nil
}

func (ls *libstore) requestRouting(key string) *rpc.Client {
	pri_key := strings.Split(key, ":")[0]
	hash_key := StoreHash(pri_key)
	for _, server_ID := range ls.nodeIDs {
		if hash_key <= server_ID {
			return ls.slave[server_ID]
		}
	}
	return ls.slave[ls.nodeIDs[0]]
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	if _, ok := ls.cache[args.Key]; ok {
		delete(ls.cache, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ls *libstore) runTime() {
	for {
		select {
		case <-ls.ticker.C:
			ls.mutex.Lock()
			for key, lease := range ls.cache {
				lease.seconds--
				if lease.seconds <= 0 {
					delete(ls.cache, key)
				} else {
					// lease.seconds = lease.seconds - 1
					// lease.seconds--
					ls.cache[key] = lease
				}
			}
			ls.mutex.Unlock()
		}
	}
}

func (ls *libstore) runTimeCount() {
	for {
		select {
		case <-ls.tickerCount.C:
			ls.mutexTime.Lock()
			for key, seconds := range ls.cacheTime {
				ls.cacheTime[key]++
				if seconds >= storagerpc.QueryCacheSeconds {
					delete(ls.cacheCount, key)
					delete(ls.cacheTime, key)
					// } else {
					// 	// ls.cacheTime[key] = seconds + 1
					//       ls.cacheTime[key]++
				}
			}
			ls.mutexTime.Unlock()
		}
	}
}

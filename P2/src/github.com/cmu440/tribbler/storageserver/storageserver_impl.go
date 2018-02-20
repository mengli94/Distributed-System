package storageserver

import (
    // "fmt"
	"errors"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"fmt"
)

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

type storageServer struct {
	kvMap          map[string]interface{}
	numOfNode      int
	listener       net.Listener
	servers        []storagerpc.Node
	nodeID         uint32
	master         *rpc.Client
	nodeMap        map[uint32]storagerpc.Node
	kvMapMutex     sync.Mutex
	mutexMap       map[string]*sync.Mutex
	mutexMap_mutex sync.Mutex
	nodeIDs        NodeIDArray
	allJoint       chan int
	leaseStatus    map[string]map[string]int
	leaseMutex     sync.Mutex
	ticker         *time.Ticker
	clientMutex    sync.Mutex
	connCache      map[string]*rpc.Client
	serversMutex   sync.Mutex
	isRevoking     map[string]bool
	revokingMutex  sync.Mutex
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	fmt.Println("start a storage server")
	fmt.Println("num of nodes:", numNodes)
	fmt.Println("node id:", nodeID)

	ss := &storageServer{
		kvMap:       make(map[string]interface{}, 1),
		mutexMap:    make(map[string]*sync.Mutex),
		numOfNode:   numNodes,
		nodeID:      nodeID,
		nodeMap:     make(map[uint32]storagerpc.Node, 1),
		nodeIDs:     NodeIDArray{},
		allJoint:    make(chan int, 1),
		leaseStatus: make(map[string]map[string]int, 1),
		ticker:      time.NewTicker(time.Millisecond * 1000),
		connCache:   make(map[string]*rpc.Client, 1),
		isRevoking:  make(map[string]bool, 1),
	}
	var err error
	if ss.listener, err = net.Listen("tcp", ":"+strconv.Itoa(port)); err != nil {
		fmt.Println("82: listerner build err", err)
		return nil, err
	}
	if err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss)); err != nil {
		fmt.Println("86: register name err", err)
		return nil, err
	}
	if len(masterServerHostPort) == 0 {
		serverNode := storagerpc.Node{
			NodeID:   nodeID,
			HostPort: "localhost:" + strconv.Itoa(port),
		}
		ss.serversMutex.Lock()
		ss.servers = append(ss.servers, serverNode)
		fmt.Println("89:length of ss.servers", len(ss.servers))
		ss.nodeMap[nodeID] = serverNode
		ss.nodeIDs = append(ss.nodeIDs, nodeID)
		ss.serversMutex.Unlock()
	}
	rpc.HandleHTTP()
	go http.Serve(ss.listener, nil)
	go ss.timeCounter()

	fmt.Println("104")

	//master storage server
	if len(masterServerHostPort) == 0 {
		if ss.numOfNode == 1 {
			return ss, nil
		}
		//check if all slave nodes are joint
		for {
			select {
			case <-ss.allJoint:
				return ss, nil
			}
		}
	} else {
		fmt.Println("node:", ss.nodeID)
		
		//slave storage server
		for {
			fmt.Println(ss.nodeID, "124:dial and call")
			ss.master, err = rpc.DialHTTP("tcp", masterServerHostPort)
			if err == nil {
				break
			}
			fmt.Println(ss.nodeID, "129:dial err")
		}
		// if ss.master, err = rpc.DialHTTP("tcp", masterServerHostPort); err != nil {
		// 	fmt.Println("119: dial err")
		// 	return nil, err
		// }
		var reply storagerpc.RegisterReply
		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{
				NodeID:   nodeID,
				HostPort: "localhost:" + strconv.Itoa(port),
			},
		}
		fmt.Println("142: prepare to call register")
		for {
            fmt.Println(ss.nodeID, ":try call register")
			if err = ss.master.Call("StorageServer.RegisterServer", args, &reply); err != nil {
				fmt.Println("146 register rpc error:", err)
				return nil, err
			}
			if reply.Status == storagerpc.OK {
				ss.serversMutex.Lock()
				ss.servers = reply.Servers
				fmt.Println("152:length of ss.servers copy", len(ss.servers))
				ss.serversMutex.Unlock()
				return ss, nil
			}
			time.Sleep(time.Millisecond * 1000)
		}
	}
	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.serversMutex.Lock()
	defer ss.serversMutex.Unlock()

	if _, ok := ss.nodeMap[args.ServerInfo.NodeID]; !ok {
		fmt.Println("167:add a new node, total length = ", len(ss.nodeMap))
		ss.nodeMap[args.ServerInfo.NodeID] = args.ServerInfo
		ss.servers = append(ss.servers, args.ServerInfo)
		ss.nodeIDs = append(ss.nodeIDs, args.ServerInfo.NodeID)
		if len(ss.nodeMap) == ss.numOfNode {
			ss.allJoint <- 0
		}
	}
	if len(ss.servers) == ss.numOfNode {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	fmt.Println("please get servers")
	ss.serversMutex.Lock()
	defer ss.serversMutex.Unlock()


	if len(ss.servers) == ss.numOfNode {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
		return nil
	}
	reply.Status = storagerpc.NotReady
	fmt.Println("196:NotReady")
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !ss.matchServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	// mutex := ss.getSingleKeyMutex(args.Key)
	// mutex.Lock()
	// defer mutex.Unlock()

	ss.kvMapMutex.Lock()
	defer ss.kvMapMutex.Unlock()

	if element, ok := ss.kvMap[args.Key]; ok {
		reply.Status = storagerpc.OK
		reply.Value, ok = element.(string)
		if !ok {
			return errors.New("Error Get")
		}
		
		//check if modifying
		ss.revokingMutex.Lock()
		revoking := ss.isRevoking[args.Key]
		ss.revokingMutex.Unlock()

		if args.WantLease {
			if !revoking {
				leaseTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
				ss.leaseMutex.Lock()
				if _, ok := ss.leaseStatus[args.Key]; !ok {
					ss.leaseStatus[args.Key] = make(map[string]int, 1)
				}
				ss.leaseStatus[args.Key][args.HostPort] = leaseTime
				ss.leaseMutex.Unlock()
				reply.Lease = storagerpc.Lease{
					Granted:      true,
					ValidSeconds: storagerpc.LeaseSeconds,
				}
			} else {
				reply.Lease = storagerpc.Lease{
					Granted:      false,
					ValidSeconds: 0,
				}

			}
			
		}

	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !ss.matchServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.revokingMutex.Lock()
	ss.isRevoking[args.Key] = true
	ss.revokingMutex.Unlock()

	mutex := ss.getSingleKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()

	ss.suspendTillAllExpired(args.Key)

	ss.kvMapMutex.Lock()
	defer ss.kvMapMutex.Unlock()

	ss.leaseMutex.Lock()
	delete(ss.leaseStatus, args.Key)
	ss.leaseMutex.Unlock()
	if _, ok := ss.kvMap[args.Key]; ok {
		delete(ss.kvMap, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}

	ss.revokingMutex.Lock()
	ss.isRevoking[args.Key] = false
	ss.revokingMutex.Unlock()
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !ss.matchServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	// mutex := ss.getSingleKeyMutex(args.Key)
	// mutex.Lock()
	// defer mutex.Unlock()

	ss.kvMapMutex.Lock()
	defer ss.kvMapMutex.Unlock()

	if element, ok := ss.kvMap[args.Key]; ok {
		reply.Status = storagerpc.OK
		reply.Value, ok = element.([]string)
		if !ok {
			return nil
		}
		
		ss.revokingMutex.Lock()
		revoking := ss.isRevoking[args.Key]
		ss.revokingMutex.Unlock()

		if args.WantLease {
			if !revoking {
				leaseTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
				ss.leaseMutex.Lock()
				if _, ok := ss.leaseStatus[args.Key]; !ok {
					ss.leaseStatus[args.Key] = make(map[string]int, 1)
				}
				ss.leaseStatus[args.Key][args.HostPort] = leaseTime
				ss.leaseMutex.Unlock()

				reply.Lease = storagerpc.Lease{
					Granted:      true,
					ValidSeconds: storagerpc.LeaseSeconds,
				}

			} else {
				reply.Lease = storagerpc.Lease{
					Granted:      false,
					ValidSeconds: 0,
				}
			}
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil

}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.matchServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.revokingMutex.Lock()
	ss.isRevoking[args.Key] = true
	ss.revokingMutex.Unlock()

	mutex := ss.getSingleKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()

	ss.suspendTillAllExpired(args.Key)

	ss.kvMapMutex.Lock()
	defer ss.kvMapMutex.Unlock()

	ss.kvMap[args.Key] = args.Value
	reply.Status = storagerpc.OK

	ss.revokingMutex.Lock()
	ss.isRevoking[args.Key] = false
	ss.revokingMutex.Unlock()

	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.matchServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.revokingMutex.Lock()
	ss.isRevoking[args.Key] = true
	ss.revokingMutex.Unlock()

	mutex := ss.getSingleKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()

	ss.suspendTillAllExpired(args.Key)

	ss.kvMapMutex.Lock()
	defer ss.kvMapMutex.Unlock()

	var list []string
	if element, ok := ss.kvMap[args.Key]; ok {
		list = element.([]string)
		for _, v := range list {
			if strings.EqualFold(v, args.Value) {
				reply.Status = storagerpc.ItemExists

				ss.revokingMutex.Lock()
				ss.isRevoking[args.Key] = false
				ss.revokingMutex.Unlock()

				return nil
			}
		}
	}
	list = append(list, args.Value)
	ss.kvMap[args.Key] = list
	reply.Status = storagerpc.OK

	ss.revokingMutex.Lock()
	ss.isRevoking[args.Key] = false
	ss.revokingMutex.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !ss.matchServer(args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.revokingMutex.Lock()
	ss.isRevoking[args.Key] = true
	ss.revokingMutex.Unlock()

	mutex := ss.getSingleKeyMutex(args.Key)
	mutex.Lock()
	defer mutex.Unlock()

	ss.suspendTillAllExpired(args.Key)

	ss.kvMapMutex.Lock()
	defer ss.kvMapMutex.Unlock()

	element, ok := ss.kvMap[args.Key]
	if !ok {
		reply.Status = storagerpc.ItemNotFound

		ss.revokingMutex.Lock()
		ss.isRevoking[args.Key] = false
		ss.revokingMutex.Unlock()

		return nil
	}
	list, ok := element.([]string)
	if !ok {
		ss.revokingMutex.Lock()
		ss.isRevoking[args.Key] = false
		ss.revokingMutex.Unlock()

		return nil
	}
	for index, v := range list {
		if strings.EqualFold(v, args.Value) {
			if len(list) == 1 {
				delete(ss.kvMap, args.Key)
			} else {
				list = append(list[:index], list[index+1:]...)
				ss.kvMap[args.Key] = list
			}
			reply.Status = storagerpc.OK
			ss.revokingMutex.Lock()
			ss.isRevoking[args.Key] = false
			ss.revokingMutex.Unlock()
			return nil
		}
	}
	reply.Status = storagerpc.ItemNotFound
	ss.revokingMutex.Lock()
	ss.isRevoking[args.Key] = false
	ss.revokingMutex.Unlock()

	return nil
}

func (ss *storageServer) timeCounter() {
	for {
		select {
		case <-ss.ticker.C:
			// fmt.Println("tick")
			ss.leaseMutex.Lock()
			for key, _ := range ss.leaseStatus {
				for addr, _ := range ss.leaseStatus[key] {
					ss.leaseStatus[key][addr]--
					if ss.leaseStatus[key][addr] <= 0 {
						delete(ss.leaseStatus[key], addr)
					}
				}
				if len(ss.leaseStatus[key]) == 0 {
					delete(ss.leaseStatus, key)
				}
			}
			ss.leaseMutex.Unlock()

		}
	}
}

func (ss *storageServer) suspendTillAllExpired(key string) {
	var seph sync.WaitGroup
	defer seph.Wait()
	var leaseHolders []string
	ss.leaseMutex.Lock()
	if addrsInLease, ok := ss.leaseStatus[key]; ok {
		for addr, leaseTime := range addrsInLease {
			if leaseTime > 0 {
				leaseHolders = append(leaseHolders, addr)
			}
		}
	}
	ss.leaseMutex.Unlock()
	for _, addr := range leaseHolders {
		seph.Add(1)
		go ss.waitToExpire(key, addr, &seph)
	}

}

func (ss *storageServer) waitToExpire(key, addr string, seph *sync.WaitGroup) {
	defer seph.Done()
	expired := make(chan int, 1)

	go func(ss *storageServer, key, addr string, expired chan int) {
		ss.clientMutex.Lock()
		c, ok := ss.connCache[addr]
		if !ok {
			var err error
			if ss.connCache[addr], err = rpc.DialHTTP("tcp", addr); err != nil {
				ss.clientMutex.Unlock()
				return
			}
			c = ss.connCache[addr]
		}
		ss.clientMutex.Unlock()

		args := &storagerpc.RevokeLeaseArgs{Key: key}
		var reply storagerpc.RevokeLeaseReply
		if err := c.Call("LeaseCallbacks.RevokeLease", args, &reply); err != nil {
			expired <- 0
			return
		}
		expired <- 0
	}(ss, key, addr, expired)
	ticker := time.NewTicker(time.Millisecond * 1000)
	for {
		select {
		case <-expired:
			ss.leaseMutex.Lock()
			if _, ok := ss.leaseStatus[key]; !ok {
				ss.leaseStatus[key] = make(map[string]int, 1)
			}
			ss.leaseStatus[key][addr] = 0
			ss.leaseMutex.Unlock()
			return
		case <-ticker.C:
			if ss.isExpired(key, addr) {
				return
			}
		}
	}
}

func (ss *storageServer) isExpired(key, addr string) bool {
	ss.leaseMutex.Lock()
	defer ss.leaseMutex.Unlock()
	if _, ok := ss.leaseStatus[key]; !ok {
		return true
	}
	if _, ok := ss.leaseStatus[key][addr]; !ok {
		return true
	}
	return ss.leaseStatus[key][addr] <= 0
}

func (ss *storageServer) matchServer(key string) bool {
	ss.serversMutex.Lock()
	defer ss.serversMutex.Unlock()
	if len(ss.nodeIDs) == 0 {
		for _, node := range ss.servers {
			ss.nodeIDs = append(ss.nodeIDs, node.NodeID)
		}
		
	}
	sort.Sort(ss.nodeIDs)
	k := strings.Split(key, ":")
	hashVal := libstore.StoreHash(k[0])
	for _, id := range ss.nodeIDs {
		if hashVal <= id {
			return ss.nodeID == id
		}
	}
	return ss.nodeID == ss.nodeIDs[0]

}

func (ss *storageServer) getSingleKeyMutex(key string) *sync.Mutex {
	ss.mutexMap_mutex.Lock()
	defer ss.mutexMap_mutex.Unlock()
	if _, ok := ss.mutexMap[key]; !ok {
		ss.mutexMap[key] = new(sync.Mutex)
	}
	keyMutex := ss.mutexMap[key]
	return keyMutex
}

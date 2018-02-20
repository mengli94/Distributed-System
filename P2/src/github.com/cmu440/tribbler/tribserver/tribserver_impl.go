package tribserver

import (
	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type tribServer struct {
	listener net.Listener
	libstore libstore.Libstore
	mutex    sync.Mutex
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	tribServer := &tribServer{}
	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	tribServer.listener = listener
	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	if tribServer.libstore, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal); err != nil {
		return nil, err
	}
	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(tribServer.listener, nil)

	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.libstore.Get(userKey); err == nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	if err := ts.libstore.Put(userKey, ""); err != nil {
		return err
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	// check if this user exists
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.libstore.Get(userKey); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	//check if the target user exists
	targetKey := util.FormatUserKey(args.TargetUserID)
	if _, err := ts.libstore.Get(targetKey); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}

	//check if target user is already been subscripted by this user
	userSubKey := util.FormatSubListKey(args.UserID)
	if err := ts.libstore.AppendToList(userSubKey, args.TargetUserID); err != nil {
		if strings.EqualFold(err.Error(), "Item Exists") {
			reply.Status = tribrpc.Exists
			return nil
		} else {
			return err
		}
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	// check if this user exists
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.libstore.Get(userKey); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	//check if the target user exists
	targetKey := util.FormatUserKey(args.TargetUserID)
	if _, err := ts.libstore.Get(targetKey); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	//check if target user is already been subscripted by this user, remove if exists
	userSubKey := util.FormatSubListKey(args.UserID)
	if err := ts.libstore.RemoveFromList(userSubKey, args.TargetUserID); err != nil {
		if strings.EqualFold(err.Error(), "Item Not Found") {
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		} else {
			return err
		}
	}
	reply.Status = tribrpc.OK
	return nil

}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	var err error
	userKey := util.FormatUserKey(args.UserID)
	if _, err = ts.libstore.Get(userKey); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	var friendIDs []string
	userSubKey := util.FormatSubListKey(args.UserID)
	subscriptions, err := ts.libstore.GetList(userSubKey)
	numOfSubs := len(subscriptions)
	for i := 0; i < numOfSubs; i++ {
		targetUserSubKey := util.FormatSubListKey(subscriptions[i])
		if targetSubs, err := ts.libstore.GetList(targetUserSubKey); err != nil {
			continue
		} else {
			for _, ID := range targetSubs {
				if ID == args.UserID {
					friendIDs = append(friendIDs, subscriptions[i])
					break
				}
			}
		}
	}
	reply.UserIDs = friendIDs
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.libstore.Get(userKey); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userTribKey := util.FormatTribListKey(args.UserID)
	timeStamp := time.Now()
	postKey := util.FormatPostKey(args.UserID, timeStamp.UnixNano())
	if err := ts.libstore.AppendToList(userTribKey, postKey); err != nil {
		return err
	}
	tribble := &tribrpc.Tribble{
		UserID:   args.UserID,
		Posted:   timeStamp,
		Contents: args.Contents,
	}

	tr, err := json.Marshal(tribble)
	if err != nil {
		return nil
	}
	err = ts.libstore.Put(postKey, string(tr))
	if err != nil {
		return err
	}
	// fmt.Println("finish the server post")
	reply.PostKey = postKey
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.libstore.Get(userKey); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userTribKey := util.FormatTribListKey(args.UserID)
	if err := ts.libstore.RemoveFromList(userTribKey, args.PostKey); err != nil {
		if strings.EqualFold(err.Error(), "Item Not Found") {
			reply.Status = tribrpc.NoSuchPost
			return nil
		} else {
			return err
		}
	}
	if err := ts.libstore.Delete(args.PostKey); err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.libstore.Get(userKey); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userTribKey := util.FormatTribListKey(args.UserID)
	postKeys, _ := ts.libstore.GetList(userTribKey)
	var tribbles []tribrpc.Tribble
	numOfPosts := len(postKeys)
	for i := numOfPosts - 1; i >= 0; i-- {
		postKey := postKeys[i]
		tr, err := ts.libstore.Get(postKey)
		var tribble tribrpc.Tribble
		json.Unmarshal([]byte(tr), &tribble)
		if err != nil {
			continue
		}
		tribbles = append(tribbles, tribble)
		if len(tribbles) >= 100 {
			break
		}
	}
	reply.Tribbles = tribbles
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()
	userKey := util.FormatUserKey(args.UserID)
	if _, err := ts.libstore.Get(userKey); err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	userSubKey := util.FormatSubListKey(args.UserID)
	targetIDs, _ := ts.libstore.GetList(userSubKey)
	numOfID := len(targetIDs)
	index := make([]int, numOfID)
	sum := numOfID
	var err error
	var tribbles []tribrpc.Tribble
	tribs := make([][]string, numOfID)
	for i := 0; i < numOfID; i++ {
		key := util.FormatTribListKey(targetIDs[i])
		tribs[i], err = ts.libstore.GetList(key)
		if err != nil {
			index[i] = -1
			sum--
			continue
		}
		index[i] = len(tribs[i]) - 1
	}
	for len(tribbles) < 100 {
		if sum <= 0 {
			break
		}
		recent := 0
		recentTrib := &tribrpc.Tribble{
			Posted: time.Unix(1334289777, 0),
		}
		for i := 0; i < numOfID; i++ {
			if index[i] < 0 {
				continue
			}
			tr, err := ts.libstore.Get(tribs[i][index[i]])
			if err != nil {
				reply.Status = tribrpc.NoSuchPost
				continue
			}
			var trib *tribrpc.Tribble
			json.Unmarshal([]byte(tr), &trib)
			if err != nil {
				continue
			}
			if recentTrib.Posted.Before(trib.Posted) {
				recent = i
				recentTrib = trib
			}
		}
		index[recent]--
		if index[recent] < 0 {
			sum--
		}
		if (recentTrib.Posted != time.Unix(1334289777, 0)) {
			tribbles = append(tribbles, *recentTrib)
		}
	}
	reply.Tribbles = tribbles
	reply.Status = tribrpc.OK
	return nil
}

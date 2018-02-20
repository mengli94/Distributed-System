package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
)

const TH = 10000

type miner struct {
	connID       int
	carryingTask *task
}

type server struct {
	lspServer lsp.Server
}

type msgFromClient struct {
	connID       int
	disconnected bool
	payload      []byte
}

type allMiners struct {
	tasks      *list.List
	idleMiners *list.List
	miners     map[int]*miner
}

type task struct {
	connID int
	req    *bitcoin.Message
}

type client struct {
	minHash    uint64
	nonce      uint64
	workerNum  int
	workerDone int
}

func startServer(port int) (*server, error) {
	s, err := lsp.NewServer(port, lsp.NewParams())
	server := &server{
		lspServer: s,
	}
	return server, err
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
		MAX  = 1024
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	msgFromC := make(chan *msgFromClient, 1)
	clients := make(map[int]*client)
	allMiners := &allMiners{
		tasks:      list.New(),
		idleMiners: list.New(),
		miners:     make(map[int]*miner, 1),
	}

	go recvClient(srv.lspServer, msgFromC)

	for {
		select {
		case message := <-msgFromC:
			if !message.disconnected {
				var msg bitcoin.Message
				err := json.Unmarshal(message.payload, &msg)
				if err != nil {
					continue
				}
				if msg.Type == bitcoin.Join {
					fmt.Println("Join")
					miner := &miner{
						connID:       message.connID,
						carryingTask: nil,
					}
					allMiners.miners[message.connID] = miner
					allMiners.idleMiners.PushBack(miner)
					assignWork(srv.lspServer, allMiners, clients)

				} else if msg.Type == bitcoin.Request {
					fmt.Println("Request")
					newTasks := breakTasks(message.connID, msg.Data, msg.Lower, msg.Upper, allMiners)
					c := &client{
						minHash:    ^uint64(0),
						workerNum:  newTasks.Len(),
						workerDone: 0,
					}
					clients[message.connID] = c
					allMiners.tasks.PushBackList(newTasks)
					assignWork(srv.lspServer, allMiners, clients)

				} else if msg.Type == bitcoin.Result {
					fmt.Println("Result")
					if miner, ok := allMiners.miners[message.connID]; ok {
						task := miner.carryingTask
						miner.carryingTask = nil
						allMiners.idleMiners.PushBack(miner)
						assignWork(srv.lspServer, allMiners, clients)
						if c, exist := clients[task.connID]; exist {
							if msg.Hash < c.minHash {
								c.minHash = msg.Hash
								c.nonce = msg.Nonce
							}
							c.workerDone++
							if c.workerDone == c.workerNum {
								fmt.Println("work done")
								result := bitcoin.NewResult(c.minHash, c.nonce)
								writeToClient(srv.lspServer, result, task.connID)
								delete(clients, task.connID)
								srv.lspServer.CloseConn(task.connID)
							}
						}
					}
				}
			} else {
				fmt.Println("enter true")
				fmt.Println(message.connID)
				if _, ok := clients[message.connID]; ok {
					fmt.Println("client dead")
					fmt.Println(message.connID)
					delete(clients, message.connID)
					srv.lspServer.CloseConn(message.connID)
				} else if miner, ok := allMiners.miners[message.connID]; ok {
					fmt.Println("miner dead")
					task := miner.carryingTask
					// fmt.Println(message.connID)
					delete(allMiners.miners, message.connID)
					deleteList(allMiners.idleMiners, miner)
					if task != nil {
						if _, ok := clients[task.connID]; ok {
							allMiners.tasks.PushBack(task)
							assignWork(srv.lspServer, allMiners, clients)
						}
					}
				}
			}
		}
	}
}

func recvClient(s lsp.Server, msgFromC chan *msgFromClient) {
	for {
		connID, payload, err := s.Read()
		m := &msgFromClient{
			connID:       connID,
			disconnected: false,
			payload:      payload,
		}

		if err != nil {
			fmt.Println(err)
			m.disconnected = true

		}
		msgFromC <- m
	}
}

func writeToClient(s lsp.Server, msg *bitcoin.Message, connID int) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = s.Write(connID, payload)
	if err != nil {
		return err
	}
	return nil
}

func assignWork(s lsp.Server, allMiners *allMiners, clients map[int]*client) {
	for m := allMiners.idleMiners.Front(); m != nil; m = m.Next() {
		idleMiner := m.Value.(*miner)
		if allMiners.tasks.Len() != 0 {
			element := allMiners.tasks.Front()
			task := element.Value.(*task)
			idleMiner.carryingTask = task
			writeToClient(s, task.req, idleMiner.connID)
			allMiners.tasks.Remove(element)
			allMiners.idleMiners.Remove(m)
		} else {
			return
		}
	}
}

func deleteList(idleMiners *list.List, miner *miner) {
	for m := idleMiners.Front(); m != nil; m = m.Next() {
		idleMiner := m.Value
		if miner == idleMiner {
			idleMiners.Remove(m)
			return
		}
	}
}

func breakTasks(connID int, message string, lower, upper uint64, allMiners *allMiners) *list.List {
	newTasks := list.New()
	totalMiners := uint64(len(allMiners.miners))
	if totalMiners == 0 {
		totalMiners = 5
	}
	unit := (upper - lower + 1) / totalMiners
	if (upper - lower + 1) < TH {
		unit = 0
	}
	if unit == 0 {
		task := &task{
			connID: connID,
			req:    bitcoin.NewRequest(message, lower, upper),
		}
		newTasks.PushBack(task)
	} else {
		var i uint64
		for i = 0; i < totalMiners; i++ {
			l := lower + unit*i
			u := l + unit - 1
			if i == totalMiners-1 {
				u = upper
			}
			task := &task{
				connID: connID,
				req:    bitcoin.NewRequest(message, l, u),
			}
			newTasks.PushBack(task)
		}
	}
	return newTasks
}

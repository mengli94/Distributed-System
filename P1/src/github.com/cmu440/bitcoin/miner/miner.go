package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return nil, err
	}

	msg := bitcoin.NewJoin()
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	client.Write(payload)
	return client, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	work := make(chan *bitcoin.Message, 1)
	go recvWork(miner, work)
	for {
		select {
		case msg := <-work:
			if msg.Type == bitcoin.Request {
				nonce := msg.Lower
				hash := bitcoin.Hash(msg.Data, msg.Lower)
				for i := msg.Lower + 1; i <= msg.Upper; i++ {
					newHash := bitcoin.Hash(msg.Data, i)
					if newHash < hash {
						hash = newHash
						nonce = i
					}
				}
				result := bitcoin.NewResult(hash, nonce)
				payload, err := json.Marshal(result)
				if err != nil {
					continue
				}
				miner.Write(payload)
			}
		}
	}
}

func recvWork(c lsp.Client, work chan *bitcoin.Message) {
	for {
		payload, err := c.Read()
		var msg bitcoin.Message
		err = json.Unmarshal(payload, &msg)
		if err != nil {
			continue
		}
		work <- &msg
	}
}

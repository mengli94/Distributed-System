// Contains the implementation of a LSP client.
package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"time"
)

const MAX = 1024

type client struct {
	addr *lspnet.UDPAddr
	ID   int
	sqn  int
	conn *lspnet.UDPConn

	//sliding window
	params      *Params
	readCommand chan int

	//receive message
	recvChan      chan *Message
	msgFromServer chan *Message
	recvOrderMap  map[int]*Message
	toReceive     *list.List
	toRecvSqn     int
	read          chan *Message

	//send message
	sendChan      chan *Message
	toResend      *list.List
	toSend        *list.List
	unAckedSqnMap map[int]int
	toAckSqn      int

	//close
	close         chan int
	closeRecieve  chan int
	wrappingUp    bool
	closeReading  chan int
	islost        chan int
	receiveClosed chan int

	// epoch
	epoch       *time.Timer
	silentEpoch int
	received    bool
	limit       chan int
}

type toResendMessage struct {
	message  *Message
	cntDown  int
	curInter int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	serverAddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return nil, err
	}
	c := buildClient(nil, 0, 0, conn, params)
	c.epoch.Reset(time.Millisecond * time.Duration(params.EpochMillis))
	c.writeToServer(NewConnect())
	data := make([]byte, MAX)
	go c.readFromServer(data)
	go c.receiveFromServer()
	for {
		select {
		case <-c.epoch.C:
			if c.silentEpoch >= params.EpochLimit {
				return nil, errors.New("Error Connection")
			}
			c.writeToServer(NewConnect())
			c.epoch.Reset(time.Millisecond * time.Duration(params.EpochMillis))
		case msg := <-c.msgFromServer:
			c.silentEpoch = 0
			if msg.Type == MsgAck && msg.SeqNum == 0 {
				c.ID = msg.ConnID
				go c.runClient()
				return c, nil
			}
		}
	}

	return c, nil
}

func (c *client) ConnID() int {
	return c.ID
}

func (c *client) Read() ([]byte, error) {
	select {
	case msg := <-c.recvChan:
		return msg.Payload, nil
	case <-c.islost:
		fmt.Println("client read error , epoch TLE")
		return nil, errors.New("The connecion is closed and no pending messages.")
	}
}

func (c *client) Write(payload []byte) error {
	c.sqn++
	c.sendChan <- NewData(c.ID, c.sqn, len(payload), payload)
	return nil
}

func (c *client) Close() error {
	c.close <- 0
	<-c.receiveClosed
	return nil
}

func buildClient(addr *lspnet.UDPAddr, id int, sqn int, conn *lspnet.UDPConn, params *Params) *client {
	c := &client{
		addr:          addr,
		ID:            id,
		sqn:           sqn,
		conn:          conn,
		params:        params,
		readCommand:   make(chan int, 1),
		recvChan:      make(chan *Message),
		msgFromServer: make(chan *Message, MAX),
		recvOrderMap:  make(map[int]*Message),
		toReceive:     list.New(),
		toRecvSqn:     1,
		sendChan:      make(chan *Message, MAX),
		toResend:      list.New(),
		toSend:        list.New(),
		unAckedSqnMap: make(map[int]int),
		toAckSqn:      1,
		close:         make(chan int, 1),
		closeRecieve:  make(chan int, 1),
		wrappingUp:    false,
		epoch:         time.NewTimer(0),
		silentEpoch:   0,
		received:      false,
		islost:        make(chan int, 1),
		receiveClosed: make(chan int, 1),
		limit:         make(chan int, 1),
		read:          make(chan *Message, 1),
		closeReading:  make(chan int, 1),
	}

	return c
}

func (c *client) runEpoch() error {
	c.silentEpoch++
	if c.silentEpoch >= c.params.EpochLimit {
		c.conn.Close()
		c.islost <- 0
		c.limit <- 0
	}
	if !c.received {
		ack := NewAck(c.ID, 0)
		c.writeToServer(ack)
	}
	for m := c.toResend.Front(); m != nil; m = m.Next() {
		toResendMsg := m.Value.(*toResendMessage)
		if toResendMsg.cntDown == 0 {
			c.writeToServer(toResendMsg.message)
			if toResendMsg.curInter == 0 {
				toResendMsg.curInter = 1
			} else if toResendMsg.curInter < c.params.MaxBackOffInterval {
				toResendMsg.curInter = toResendMsg.curInter * 2
			}
			toResendMsg.cntDown = toResendMsg.curInter
		} else {
			toResendMsg.cntDown--
		}
	}
	c.received = false
	c.epoch.Reset(time.Millisecond * time.Duration(c.params.EpochMillis))
	return nil
}

func (c *client) writeToServer(msg *Message) error {
	packet, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(packet)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) readFromServer(data []byte) {
	for {
		select {
		case <-c.closeReading:
			c.receiveClosed <- 0
			return
		default:
			n, err := c.conn.Read(data)
			if err != nil {
				continue
			}
			var msg Message
			err = json.Unmarshal(data[:n], &msg)
			if err != nil {
				continue
			}
			if len(msg.Payload) < msg.Size {
				continue
			} else if len(msg.Payload) > msg.Size {
				msg.Payload = msg.Payload[:msg.Size]
			}
			c.read <- &msg
		}
	}

}

func (c *client) receiveFromServer() {
	for {
		select {
		case <-c.closeRecieve:
			c.closeReading <- 0
			return
		case msg := <-c.read:
			c.msgFromServer <- msg
			if msg.Type == MsgData {
				ack := NewAck(c.ID, msg.SeqNum)
				c.writeToServer(ack)
			}
		}
	}

}

func (c *client) inOrderSend() {
	for m := c.toSend.Front(); m != nil; m = m.Next() {
		message := m.Value.(*Message)
		if message.SeqNum >= (c.toAckSqn + c.params.WindowSize) {
			return
		}
		c.writeToServer(message)
		c.unAckedSqnMap[message.SeqNum] = 0
		c.toSend.Remove(m)
		toResendMsg := &toResendMessage{
			message,
			0,
			0,
		}
		c.toResend.PushBack(toResendMsg)
	}
}

func (c *client) runClient() {
	c.epoch.Reset(time.Millisecond * time.Duration(c.params.EpochMillis))
	for {
		if c.toReceive.Len() != 0 {
			e := c.toReceive.Front()
			select {
			case <-c.close:
				if c.AllPendingDone() {
					c.closeRecieve <- 0
					return
				}
			case <-c.limit:
				c.closeRecieve <- 0
				return
			case c.recvChan <- e.Value.(*Message):
				c.toReceive.Remove(e)
			case <-c.epoch.C:
				c.runEpoch()
			case msg := <-c.msgFromServer:
				if msg.Type == MsgAck {
					c.silentEpoch = 0
					delete(c.unAckedSqnMap, msg.SeqNum)
					for m := c.toResend.Front(); m != nil; m = m.Next() {
						message := m.Value.(*toResendMessage).message
						if _, exist := c.unAckedSqnMap[message.SeqNum]; exist {
							break
						}
						c.toAckSqn++
						c.toResend.Remove(m)
					}
					c.inOrderSend()
				} else if msg.Type == MsgData {
					c.silentEpoch = 0
					c.received = true
					if _, exist := c.recvOrderMap[msg.SeqNum]; !exist && msg.SeqNum >= c.toRecvSqn {
						c.recvOrderMap[msg.SeqNum] = msg
						sqn := c.toRecvSqn
						for {
							if _, exist := c.recvOrderMap[sqn]; exist {
								c.toRecvSqn++
								c.toReceive.PushBack(c.recvOrderMap[sqn])
								delete(c.recvOrderMap, sqn)
								sqn++
							} else {
								break
							}
						}
					}
					if c.wrappingUp {
						if c.AllPendingDone() {
							c.closeRecieve <- 0
							return
						}
						continue
					}
				}
			case msg := <-c.sendChan:
				c.toSend.PushBack(msg)
				c.inOrderSend()
			}
		} else {
			select {
			case <-c.close:
				if c.AllPendingDone() {
					c.closeRecieve <- 0
					return
				}
			case <-c.limit:
				c.closeRecieve <- 0
				return
			case <-c.epoch.C:
				c.runEpoch()
			case msg := <-c.msgFromServer:
				if msg.Type == MsgAck {
					c.silentEpoch = 0
					delete(c.unAckedSqnMap, msg.SeqNum)
					for m := c.toResend.Front(); m != nil; m = m.Next() {
						message := m.Value.(*toResendMessage).message
						if _, exist := c.unAckedSqnMap[message.SeqNum]; exist {
							break
						}
						c.toAckSqn++
						c.toResend.Remove(m)
					}
					c.inOrderSend()
				} else if msg.Type == MsgData {
					c.silentEpoch = 0
					c.received = true
					if _, exist := c.recvOrderMap[msg.SeqNum]; !exist && msg.SeqNum >= c.toRecvSqn {
						c.recvOrderMap[msg.SeqNum] = msg
						sqn := c.toRecvSqn
						for {
							if _, exist := c.recvOrderMap[sqn]; exist {
								c.toRecvSqn++
								c.toReceive.PushBack(c.recvOrderMap[sqn])
								delete(c.recvOrderMap, sqn)
								sqn++
							} else {
								break
							}
						}
					}
					if c.wrappingUp {
						continue
					}
				}
				if c.wrappingUp {
					if c.AllPendingDone() {
						c.closeRecieve <- 0
						return
					}
				}
			case msg := <-c.sendChan:
				c.toSend.PushBack(msg)
				c.inOrderSend()
			}
		}
	}
}

func (c *client) AllPendingDone() bool {
	c.wrappingUp = true
	if c.toSend.Len() == 0 && c.toResend.Len() == 0 && len(c.unAckedSqnMap) == 0 && len(c.sendChan) == 0 {
		if c.conn != nil {
			c.conn.Close()
		}
		return true
	}
	return false

}

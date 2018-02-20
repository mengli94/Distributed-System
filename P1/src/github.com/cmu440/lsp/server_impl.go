// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type readMsg struct {
	msg        *Message
	clientAddr *lspnet.UDPAddr
}

type writeMsg struct {
	connID  int
	payload []byte
}

type server struct {
	listener    *lspnet.UDPConn
	params      *Params
	IDMap       map[int]*client
	deadID      chan int
	ID          int
	readCommand chan int

	//close
	close          chan int
	closeRecieving chan int
	wrappingUp     bool
	toWrapUp       chan int
	closeReading   chan int

	//receive
	recvChan chan *Message
	addrMap  map[string]int

	//some client dead
	clientDead    chan int
	IDMapEmpty    chan int
	receiveClosed chan int

	//check ID map
	checkIDexist chan int
	checkIDDone  chan *client
	IDnotExist   chan int

	//read and write
	readMsg chan *readMsg
	write   chan *writeMsg
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	hostport := lspnet.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	laddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	listener, err := lspnet.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}
	s := &server{
		listener:       listener,
		params:         params,
		IDMap:          make(map[int]*client),
		deadID:         make(chan int, MAX),
		ID:             1,
		readCommand:    make(chan int, 1),
		close:          make(chan int, 1),
		closeRecieving: make(chan int, 1),
		wrappingUp:     false,
		toWrapUp:       make(chan int, 1),
		recvChan:       make(chan *Message),
		addrMap:        make(map[string]int),
		clientDead:     make(chan int, 1),
		IDMapEmpty:     make(chan int, 1),
		receiveClosed:  make(chan int, 1),
		checkIDexist:   make(chan int, 1),
		checkIDDone:    make(chan *client, 1),
		IDnotExist:     make(chan int, 1),
		readMsg:        make(chan *readMsg, 1),
		closeReading:   make(chan int, 1),
		write:          make(chan *writeMsg, 1),
	}
	data := make([]byte, MAX)
	go s.readFromClient(data)
	go s.receivingFromClient()
	return s, err
}

func (s *server) Read() (int, []byte, error) {
	select {
	case msg, ok := <-s.recvChan:
		if !ok {
			return 0, nil, errors.New("client is closed")
		}
		return msg.ConnID, msg.Payload, nil

	case ID := <-s.clientDead:
		return ID, nil, errors.New("connection to client is closed or lost!")
	}
}

func (s *server) Write(connID int, payload []byte) error {
	s.write <- &writeMsg{
		connID,
		payload,
	}
	select {
	case <-s.checkIDDone:
		return nil
	case <-s.IDnotExist:
		return errors.New("The connecion with client has lost.")
	}
}

func (s *server) CloseConn(connID int) error {
	s.checkIDexist <- connID
	select {
	case c := <-s.checkIDDone:
		c.close <- 0
		return nil
	case <-s.IDnotExist:
		return errors.New("The connecion with client has lost.")
	}
}

func (s *server) Close() error {
	s.toWrapUp <- 0
	<-s.IDMapEmpty
	s.closeRecieving <- 0
	<-s.receiveClosed
	return nil
}

func (s *server) writeToClient(msg *Message, clientAddr *lspnet.UDPAddr) error {
	packet, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = s.listener.WriteToUDP(packet, clientAddr)
	if err != nil {
		return err
	}
	return nil
}

func (s *server) readFromClient(data []byte) {
	for {
		select {
		case <-s.closeReading:
			s.receiveClosed <- 0
			return
		default:
			n, clientAddr, err := s.listener.ReadFromUDP(data)
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
			s.readMsg <- &readMsg{
				&msg,
				clientAddr,
			}
		}
	}
}

func (s *server) receivingFromClient() {
	for {
		select {
		case id := <-s.checkIDexist:
			c, ok := s.IDMap[id]
			if ok {
				s.checkIDDone <- c
			} else {
				s.IDnotExist <- 0
			}
		case msg := <-s.write:
			c, ok := s.IDMap[msg.connID]
			if ok {
				c.sendChan <- NewData(msg.connID, c.sqn, len(msg.payload), msg.payload)
				c.sqn++
				s.checkIDDone <- c
			} else {
				s.IDnotExist <- 0
			}
		case <-s.toWrapUp:
			s.wrappingUp = true
			for _, c := range s.IDMap {
				c.close <- 0
			}
		case <-s.closeRecieving:
			s.listener.Close()
			s.closeReading <- 0
			return
		case dead := <-s.deadID:
			c := s.IDMap[dead]
			delete(s.IDMap, dead)
			delete(s.addrMap, c.addr.String())
			if len(s.IDMap) == 0 {
				s.IDMapEmpty <- 0
			}
		case readMsg := <-s.readMsg:
			msg := readMsg.msg
			clientAddr := readMsg.clientAddr
			if msg.Type == MsgConnect && !s.wrappingUp {
				if _, exist := s.addrMap[clientAddr.String()]; exist {
					continue
				} else {
					s.addrMap[clientAddr.String()] = 0
				}
				c := buildClient(clientAddr, s.ID, 1, s.listener, s.params)
				ackToConn := NewAck(c.ID, 0)
				s.writeToClient(ackToConn, c.addr)
				s.IDMap[c.ID] = c
				s.ID++
				go s.runServerToClient(c)
			} else if msg.Type == MsgData && !s.wrappingUp {
				ack := NewAck(msg.ConnID, msg.SeqNum)
				s.writeToClient(ack, clientAddr)
				s.IDMap[msg.ConnID].msgFromServer <- msg
			} else if msg.Type == MsgAck {
				if _, ok := s.IDMap[msg.ConnID]; ok {
					s.IDMap[msg.ConnID].msgFromServer <- msg
				}
			}
		}
	}
}

func (s *server) inOrderSend(c *client) {
	for m := c.toSend.Front(); m != nil; m = m.Next() {
		message := m.Value.(*Message)
		if message.SeqNum >= (c.toAckSqn + c.params.WindowSize) {
			return
		}
		s.writeToClient(message, c.addr)
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

func (s *server) runEpoch(c *client) error {
	c.silentEpoch++
	if c.silentEpoch >= c.params.EpochLimit {
		s.clientDead <- c.ID
		c.limit <- 0
		s.deadID <- c.ID
		return errors.New("Error Connection")
	}
	if !c.received {
		ack := NewAck(c.ID, 0)
		s.writeToClient(ack, c.addr)
	}
	for m := c.toResend.Front(); m != nil; m = m.Next() {
		toResendMsg := m.Value.(*toResendMessage)
		if toResendMsg.cntDown == 0 {
			s.writeToClient(toResendMsg.message, c.addr)
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

func (s *server) runServerToClient(c *client) {
	c.epoch.Reset(time.Millisecond * time.Duration(c.params.EpochMillis))
	for {
		if c.toReceive.Len() != 0 {
			e := c.toReceive.Front()
			select {
			case <-c.close:
				if c.AllPending(s) {
					return
				}
			case <-c.limit:
				return
			case s.recvChan <- e.Value.(*Message):
				c.toReceive.Remove(e)
			case <-c.epoch.C:
				s.runEpoch(c)
			case msg := <-c.msgFromServer:
				c.silentEpoch = 0
				if msg.Type == MsgAck {
					delete(c.unAckedSqnMap, msg.SeqNum)
					for m := c.toResend.Front(); m != nil; m = m.Next() {
						message := m.Value.(*toResendMessage).message
						if _, exist := c.unAckedSqnMap[message.SeqNum]; exist {
							break
						}
						c.toAckSqn++
						c.toResend.Remove(m)
					}
					s.inOrderSend(c)
				} else if msg.Type == MsgData {
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
				}
			case msg := <-c.sendChan:
				c.toSend.PushBack(msg)
				s.inOrderSend(c)
			}
		} else {
			select {
			case <-c.close:
				if c.AllPending(s) {
					return
				}
			case <-c.limit:
				return
			case <-c.epoch.C:
				s.runEpoch(c)
			case msg := <-c.msgFromServer:
				c.silentEpoch = 0
				if msg.Type == MsgAck {
					delete(c.unAckedSqnMap, msg.SeqNum)
					for m := c.toResend.Front(); m != nil; m = m.Next() {
						message := m.Value.(*toResendMessage).message
						if _, exist := c.unAckedSqnMap[message.SeqNum]; exist {
							break
						}
						c.toAckSqn++
						c.toResend.Remove(m)
					}
					s.inOrderSend(c)
				} else if msg.Type == MsgData {
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
				}
				if c.wrappingUp {
					if c.AllPending(s) {
						return
					}
				}
			case msg := <-c.sendChan:
				c.toSend.PushBack(msg)
				s.inOrderSend(c)
			}
		}
	}
}

func (c *client) AllPending(s *server) bool {
	c.wrappingUp = true
	if c.toSend.Len() == 0 && c.toResend.Len() == 0 && len(c.unAckedSqnMap) == 0 && len(c.sendChan) == 0 {
		s.deadID <- c.ID
		return true
	}
	return false
}

package p0

import (
	"bufio"
	"net"
	"strconv"
)

const DEFAULT_SIZE = 500

type keyValueServer struct {
	listener    net.Listener
	clients     []*Client
	dead        chan *Client
	count       chan int
	nowClients  chan int
	connections chan net.Conn
	requests    chan *Request
	close       chan int
}

type Client struct {
	connection net.Conn
	messages   chan []byte
	close      chan int
}

type Request struct {
	client *Client
	reType string
	key    string
	value  []byte
}

func New() KeyValueServer {
	init_db()
	return &keyValueServer{
		nil,
		nil,
		make(chan *Client),
		make(chan int),
		make(chan int),
		make(chan net.Conn),
		make(chan *Request),
		make(chan int)}
}

func (kvs *keyValueServer) Start(port int) error {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return err
	}
	kvs.listener = ln

	go func() {
		for {
			select {
			case <-kvs.close:
				kvs.close <- 0
				return
			default:
				conn, err := kvs.listener.Accept()
				if err == nil {
					kvs.connections <- conn
				}
			}
		}
	}()
	go run(kvs)

	return nil
}

func (kvs *keyValueServer) Close() {
	kvs.listener.Close()
	kvs.close <- 0
}

func (kvs *keyValueServer) Count() int {
	kvs.count <- 0
	return <-kvs.nowClients
}

func run(kvs *keyValueServer) {
	for {
		select {

		case <-kvs.close:
			for _, c := range kvs.clients {
				c.connection.Close()
				c.close <- 0
			}
			return

		case <-kvs.count:
			kvs.nowClients <- len(kvs.clients)

		case connection := <-kvs.connections:
		 	c := &Client{
				connection,
				make(chan []byte, DEFAULT_SIZE),
				make(chan int),
			}
			kvs.clients = append(kvs.clients, c)
			go read(kvs, c)
			go write(c)

		case dead := <-kvs.dead:
			for i, c := range kvs.clients {
				if c == dead {
					kvs.clients = append(kvs.clients[:i], kvs.clients[i+1:]...)
					break
				}
			}

		case request := <-kvs.requests:
			if request.reType == "get" {
				v := get(request.key)
				keyBytes := []byte(request.key)
				if len(request.client.messages) == DEFAULT_SIZE {
					<-request.client.messages
				}
				for _, c := range v {
					request.client.messages <- append(append(keyBytes, ","...), c...)
				}
			} else {
				put(request.key, request.value)
			}
		}
	}
}

func read(kvs *keyValueServer, client *Client) {
	reader := bufio.NewReader(client.connection)
	for {
		select {
		case <-client.close:
			client.close <- 0
			return
		default:
			message, err := reader.ReadBytes('\n')
			if err != nil {
				kvs.dead <- client
			} else {
				newMessage := string(message)
				var i, j int
				for i = range newMessage {
					if newMessage[i] == ',' {
						break
					}
				}
				for j = i + 1; j < len(newMessage); j++ {
					if newMessage[j] == ',' {
						break
					}
				}
				if newMessage[:i] == "put" {
					key := newMessage[i+1 : j]
					kvs.requests <- &Request{
						client,
						"put",
						key,
						[]byte(newMessage[j+1:])}
				} else {
					key := newMessage[i+1 : len(newMessage)-1]
					kvs.requests <- &Request{
						client,
						"get",
						key,
						nil}
				}
			}
		}
	}
}

func write(client *Client) {
	for {
		select {
		case <-client.close:
			client.close <- 0
			return
		case message := <-client.messages:
			client.connection.Write(message)
		}
	}
}

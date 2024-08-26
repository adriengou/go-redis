package main

import (
	"context"
	"fmt"
	"github.com/adriengou/go-redis/src/client"
	"log"
	"log/slog"
	"net"
	"time"
)

const defaultListenAddress = ":3333"

type Config struct {
	ListenAddress string
}

type Server struct {
	Config
	ln net.Listener

	peers map[*Peer]bool

	addPeerCh chan *Peer
	quitCh    chan struct{}
	msgCh     chan []byte

	kv *KV
}

func NewServer(cfg Config) *Server {

	if len(cfg.ListenAddress) == 0 {
		cfg.ListenAddress = defaultListenAddress
	}

	return &Server{
		Config:    cfg,
		peers:     make(map[*Peer]bool),
		addPeerCh: make(chan *Peer),
		quitCh:    make(chan struct{}),
		msgCh:     make(chan []byte),
		kv:        NewKV(),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.ListenAddress)
	if err != nil {
		return err
	}

	s.ln = ln
	go s.loop()

	slog.Info("server running", "listenAddress", s.ListenAddress)

	return s.acceptLoop()
}

func (s *Server) loop() {
	for {
		select {
		case <-s.quitCh:
			return

		case peer := <-s.addPeerCh:
			s.peers[peer] = true

		case msgBuf := <-s.msgCh:
			//fmt.Println(string(msgBuf))
			err := s.handleRawMessage(msgBuf)
			if err != nil {
				slog.Error("raw message error", "err", err)
			}
		}
	}
}

func (s *Server) acceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			slog.Error("accept error", "err", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	peer := NewPeer(conn, s.msgCh)
	s.addPeerCh <- peer
	slog.Info("new peer connected", "remoteAddr", conn.RemoteAddr())
	if err := peer.readLoop(); err != nil {
		slog.Error("peer read error", "err", err, "remoteAdd", conn.RemoteAddr())
	}
}

func (s *Server) handleRawMessage(msgBuf []byte) error {
	cmd, err := parseCommand(string(msgBuf))
	if err != nil {
		return err
	}

	switch v := cmd.(type) {
	case SetCommand:
		fmt.Println("wants to set a key in to hash table", "key", v.key, "val", v.val)
		return s.kv.Set(v.key, v.val)
	}
	return nil
}

func main() {

	server := NewServer(Config{})
	go func() {
		log.Fatal(server.Start())
	}()

	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {

		c := client.NewClient("localhost:3333")
		err := c.Set(context.TODO(), fmt.Sprint("foo_", i), fmt.Sprint("bar_", i))
		if err != nil {
			log.Fatal(err)
		}
	}

	time.Sleep(time.Second)
	fmt.Println(server.kv.data)

	select {} // blocking so the program don't stop
}

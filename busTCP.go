package busTCP

import (
	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/bus"
	"github.com/go-mangos/mangos/transport/tcp"
	"github.com/IntelliQru/logger"
)

type Cluster struct {
	sock mangos.Socket
	logger *logger.Logger
}

func NewCluster(l *logger.Logger) *Cluster {
	cluster := Cluster{
		logger:l,
	}
	cluster.sock , _ = bus.NewSocket()
	cluster.sock.AddTransport(tcp.NewTransport())

	return &cluster
}

func (c *Cluster) Listen(addr string, handler func(d []byte)) error {
	err := c.sock.Listen(addr);
	if err != nil {
		return err
	}

	go func(c *Cluster) {
		defer c.sock.Close()
		for {
			data, err := c.sock.Recv();
			if err != nil {
				c.logger.Error(err)
				return
			}

			go handler(data)
		}
	}(c)

	return nil
}

func (c *Cluster) ConnectToNodes(nodeAddrs... string) error{
	for _, addr := range nodeAddrs {
		err := c.sock.Dial(addr);
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) Send(data []byte) error {
	err := c.sock.Send(data)
	return err
}
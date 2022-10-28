package natscl

import (
	"context"
	"strings"
	"time"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
)

var log = logging.Log.WithFields(logrus.Fields{"package": "natscl"})

type ConnectionSettings struct {
	ClusterURLS   string
	CredsPath     string
	TLSCACertPath string
	TLSCertPath   string
	TLSKeyPath    string
	MaxReconnects int
	ReconnectWait int
}

func NewConnection(settings *ConnectionSettings) (*nats.EncodedConn, error) {
	log := log.WithFields(logrus.Fields{"context": "new nats conn"})

	nc, err := nats.Connect(
		settings.ClusterURLS,
		nats.UserCredentials(settings.CredsPath),
		nats.RootCAs(settings.TLSCACertPath),
		nats.ClientCert(settings.TLSCertPath, settings.TLSKeyPath),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(settings.MaxReconnects),
		nats.ReconnectWait(time.Duration(settings.ReconnectWait)*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Errorf("disconnected from nats: %s", err.Error())
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Infof("reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Errorf("connection closed: %s", nc.LastError().Error())
		}),
	)
	if err != nil {
		return nil, err
	}

	encConn, err := nats.NewEncodedConn(nc, "protojson")
	if err != nil {
		return nil, err
	}

	return encConn, nil
}

type Client struct {
	conn          *nats.EncodedConn
	subscriptions []*nats.Subscription
	userSuffix    string
	queueSuffix   string
}

func NewClient(conn *nats.EncodedConn, userSuffix, queueSuffix string) *Client {
	return &Client{
		conn:          conn,
		userSuffix:    userSuffix,
		queueSuffix:   queueSuffix,
		subscriptions: make([]*nats.Subscription, 0),
	}
}

func (c *Client) queueName(base string) string {
	return strings.Join([]string{base, c.queueSuffix}, ".")
}

func (c *Client) Subscribe(subject string, handler nats.Handler) error {
	queue := c.queueName(subject)

	s, err := c.conn.QueueSubscribe(subject, queue, handler)
	if err != nil {
		return err
	}

	c.subscriptions = append(c.subscriptions, s)

	log.Infof("added handler for subject %s on queue %s", subject, queue)

	return nil
}

func (c *Client) Respond(ctx context.Context, replySubject string, response gotelnats.DEResponse) error {
	return gotelnats.PublishResponse(ctx, c.conn, replySubject, response)
}

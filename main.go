package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cyverse-de/go-mod/cfg"
	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/go-mod/otelutils"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/cyverse-de/go-mod/subjects"
	"github.com/cyverse-de/subscriptions/natscl"
	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	_ "expvar"

	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/lib/pq"
)

const serviceName = "subscriptions"

var log = logging.Log.WithFields(logrus.Fields{"package": "main"})

func main() {
	var (
		err    error
		config *koanf.Koanf
		dbconn *sqlx.DB

		configPath     = flag.String("config", cfg.DefaultConfigPath, "Path to the config file")
		dotEnvPath     = flag.String("dotenv-path", cfg.DefaultDotEnvPath, "Path to the dotenv file")
		tlsCert        = flag.String("tlscert", gotelnats.DefaultTLSCertPath, "Path to the NATS TLS cert file")
		tlsKey         = flag.String("tlskey", gotelnats.DefaultTLSKeyPath, "Path to the NATS TLS key file")
		caCert         = flag.String("tlsca", gotelnats.DefaultTLSCAPath, "Path to the NATS TLS CA file")
		credsPath      = flag.String("creds", gotelnats.DefaultCredsPath, "Path to the NATS creds file")
		maxReconnects  = flag.Int("max-reconnects", gotelnats.DefaultMaxReconnects, "Maximum number of reconnection attempts to NATS")
		reconnectWait  = flag.Int("reconnect-wait", gotelnats.DefaultReconnectWait, "Seconds to wait between reconnection attempts to NATS")
		natsSubject    = flag.String("subject", "cyverse.qms.>", "NATS subject to subscribe to")
		natsQueue      = flag.String("queue", "cyverse.qms", "Name of the NATS queue to use")
		envPrefix      = flag.String("env-prefix", "QMS_", "The prefix for environment variables")
		reportOverages = flag.Bool("report-overages", true, "Allows the overages feature to effectively be shut down")
		logLevel       = flag.String("log-level", "debug", "One of trace, debug, info, warn, error, fatal, or panic.")
		listenPort     = flag.Int("port", 60000, "The port the service listens on for requests")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	log := log.WithFields(logrus.Fields{"context": "main"})

	var tracerCtx, cancel = context.WithCancel(context.Background())
	defer cancel()
	shutdown := otelutils.TracerProviderFromEnv(tracerCtx, serviceName, func(e error) { log.Fatal(e) })
	defer shutdown()

	nats.RegisterEncoder("protojson", protobufjson.NewCodec(protobufjson.WithEmitUnpopulated()))

	config, err = cfg.Init(&cfg.Settings{
		EnvPrefix:   *envPrefix,
		ConfigPath:  *configPath,
		DotEnvPath:  *dotEnvPath,
		StrictMerge: false,
		FileType:    cfg.YAML,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("Done reading config from %s", *configPath)

	dbURI := config.String("database.uri")
	if dbURI == "" {
		log.Fatal("database.uri must be set in the configuration file")
	}

	// Make sure the db.uri URL is parseable
	if _, err = url.Parse(dbURI); err != nil {
		log.Fatal(errors.Wrap(err, "Can't parse database.uri in the config file"))
	}

	userSuffix := config.String("username.suffix")
	if userSuffix == "" {
		log.Fatal("users.domain must be set in the configuration file")
	}

	natsCluster := config.String("nats.cluster")
	if natsCluster == "" {
		log.Fatalf("The %sNATS_CLUSTER environment variable or nats.cluster configuration value must be set", *envPrefix)
	}

	dbconn = otelsqlx.MustConnect("postgres", dbURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL))
	log.Info("done connecting to the database")
	dbconn.SetMaxOpenConns(10)
	dbconn.SetConnMaxIdleTime(time.Minute)

	natsSettings := natscl.ConnectionSettings{
		ClusterURLS:   natsCluster,
		CredsPath:     *credsPath,
		TLSCACertPath: *caCert,
		TLSCertPath:   *tlsCert,
		TLSKeyPath:    *tlsKey,
		MaxReconnects: *maxReconnects,
		ReconnectWait: *reconnectWait,
	}

	natsConn, err := natscl.NewConnection(&natsSettings)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("configured servers: %s", strings.Join(natsConn.Conn.Servers(), " "))
	log.Infof("connected to NATS host: %s", natsConn.Conn.ConnectedServerName())
	log.Infof("NATS URLs are %s", natsSettings.ClusterURLS)
	log.Infof("NATS TLS cert file is %s", natsSettings.TLSCertPath)
	log.Infof("NATS TLS key file is %s", natsSettings.TLSKeyPath)
	log.Infof("NATS CA cert file is %s", natsSettings.TLSCACertPath)
	log.Infof("NATS creds file is %s", natsSettings.CredsPath)
	log.Infof("NATS subject is %s", *natsSubject)
	log.Infof("NATS queue is %s", *natsQueue)
	log.Infof("--report-overages is %t", *reportOverages)

	natsClient := natscl.NewClient(natsConn, userSuffix, serviceName)

	app := New(natsClient, dbconn, userSuffix)

	natsClient.Subscribe(subjects.QMSGetUserUpdates, app.GetUserUpdatesHandler)
	natsClient.Subscribe(subjects.QMSAddUserUpdate, app.AddUserUpdateHandler)

	// Only call these two endpoints if you need to correct a usage value and
	// bypass the updates tables.
	natsClient.Subscribe(subjects.QMSGetUserUsages, app.GetUsagesHandler)
	natsClient.Subscribe(subjects.QMSAddUserUsages, app.AddUsageHandler)

	// These will get used by frontend calls to check for user overages.
	natsClient.Subscribe(subjects.QMSGetUserOverages, app.GetUserOverages)
	natsClient.Subscribe(subjects.QMSCheckUserOverages, app.CheckUserOverages)

	srv := fmt.Sprintf(":%s", strconv.Itoa(*listenPort))
	log.Fatal(http.ListenAndServe(srv, nil))
}

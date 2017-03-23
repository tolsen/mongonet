package mongonet

import "crypto/x509"
import "fmt"
import "time"

import "github.com/mongodb/slogger/v2/slogger"

type SSLPair struct {
	CertFile string
	KeyFile  string
}

type ProxyConfig struct {
	BindHost string
	BindPort int

	MongoHost          string
	MongoPort          int
	MongoSSL           bool
	MongoRootCAs       *x509.CertPool
	MongoSSLSkipVerify bool

	UseSSL  bool
	SSLKeys []SSLPair

	LogLevel  slogger.Level
	Appenders []slogger.Appender

	InterceptorFactory ProxyInterceptorFactory

	ConnectionPoolHook ConnectionHook

	TCPKeepAlivePeriod time.Duration // set to 0 for no keep alives
}

func NewProxyConfig(bindHost string, bindPort int, mongoHost string, mongoPort int) ProxyConfig {
	return ProxyConfig{
		bindHost,
		bindPort,
		mongoHost,
		mongoPort,
		false, // MongoSSL
		nil,   // MongoRootCAs
		false, // MongoSSLSkipVerify
		false, // UseSSL
		nil,
		slogger.OFF, // LogLevel
		nil,         // Appenders
		nil,         // InterceptorFactory
		nil,         // ConnectionPoolHook
		0,           // TCPKeepAlivePeriod
	}
}

func (pc *ProxyConfig) MongoAddress() string {
	return fmt.Sprintf("%s:%d", pc.MongoHost, pc.MongoPort)
}

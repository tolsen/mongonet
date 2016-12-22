package mongonet

import "crypto/x509"
import "fmt"

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
	}
}

func (pc *ProxyConfig) MongoAddress() string {
	return fmt.Sprintf("%s:%d", pc.MongoHost, pc.MongoPort)
}

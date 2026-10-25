// Package tlsutil builds gRPC transport credentials for mutual TLS between
// nodes and for clients talking to the cluster.
package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

type Files struct {
	Cert string
	Key  string
	CA   string
}

func (f Files) Enabled() bool { return f.Cert != "" || f.Key != "" || f.CA != "" }

func (f Files) load() (tls.Certificate, *x509.CertPool, error) {
	if f.Cert == "" || f.Key == "" || f.CA == "" {
		return tls.Certificate{}, nil, errors.New("tls: cert, key and ca files must all be set")
	}
	cert, err := tls.LoadX509KeyPair(f.Cert, f.Key)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("tls: load key pair: %w", err)
	}
	pem, err := os.ReadFile(f.CA)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("tls: read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return tls.Certificate{}, nil, errors.New("tls: ca file has no certificates")
	}
	return cert, pool, nil
}

// Server demands a client certificate signed by the same CA.
func (f Files) Server() (credentials.TransportCredentials, error) {
	cert, pool, err := f.load()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

func (f Files) Client() (credentials.TransportCredentials, error) {
	cert, pool, err := f.load()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

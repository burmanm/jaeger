// Copyright (c) 2018 The Jaeger Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpc

import (
	"io"

	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/jaegertracing/jaeger/cmd/agent/app/configmanager"
	grpcManager "github.com/jaegertracing/jaeger/cmd/agent/app/configmanager/grpc"
	"github.com/jaegertracing/jaeger/cmd/agent/app/reporter"
	aReporter "github.com/jaegertracing/jaeger/cmd/agent/app/reporter"
)

// ProxyBuilder holds objects communicating with collector
type ProxyBuilder struct {
	reporter aReporter.Reporter
	manager  configmanager.ClientConfigManager
	conn     *grpc.ClientConn
}

// NewCollectorProxy creates ProxyBuilder
func NewCollectorProxy(builder *ConnBuilder, opts *reporter.Options, mFactory metrics.Factory, logger *zap.Logger) (*ProxyBuilder, error) {
	conn, err := builder.CreateConnection(logger)
	if err != nil {
		return nil, err
	}
	grpcMetrics := mFactory.Namespace(metrics.NSOptions{Name: "", Tags: map[string]string{"protocol": "grpc"}})
	return &ProxyBuilder{
		conn:     conn,
		reporter: reporter.WrapWithQueue(opts, NewReporter(conn, opts, logger), logger, grpcMetrics),
		manager:  configmanager.WrapWithMetrics(grpcManager.NewConfigManager(conn), grpcMetrics),
	}, nil
}

// GetConn returns grpc conn
func (b ProxyBuilder) GetConn() *grpc.ClientConn {
	return b.conn
}

// GetReporter returns Reporter
func (b ProxyBuilder) GetReporter() aReporter.Reporter {
	return b.reporter
}

// GetManager returns manager
func (b ProxyBuilder) GetManager() configmanager.ClientConfigManager {
	return b.manager
}

// Close closes connections used by proxy.
func (b ProxyBuilder) Close() error {
	var err error
	if closer, ok := b.reporter.(io.Closer); ok {
		err = closer.Close()
	}
	// Even if the previous errored, we want to close the conn
	err2 := b.conn.Close()
	if err != nil {
		return err
	}
	return err2
}

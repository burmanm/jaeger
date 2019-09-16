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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jaegertracing/jaeger/cmd/agent/app/reporter"
	"github.com/jaegertracing/jaeger/cmd/agent/app/reporter/common"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/proto-gen/api_v2"
	jThrift "github.com/jaegertracing/jaeger/thrift-gen/jaeger"
	"github.com/jaegertracing/jaeger/thrift-gen/zipkincore"
)

type mockSpanHandler struct {
	mux      sync.Mutex
	requests []*api_v2.PostSpansRequest
}

func (h *mockSpanHandler) getRequests() []*api_v2.PostSpansRequest {
	h.mux.Lock()
	defer h.mux.Unlock()
	return h.requests
}

func (h *mockSpanHandler) PostSpans(c context.Context, r *api_v2.PostSpansRequest) (*api_v2.PostSpansResponse, error) {
	h.mux.Lock()
	defer h.mux.Unlock()
	h.requests = append(h.requests, r)
	return &api_v2.PostSpansResponse{}, nil
}

func TestReporter_EmitZipkinBatch(t *testing.T) {
	handler := &mockSpanHandler{}
	s, addr := initializeGRPCTestServer(t, func(s *grpc.Server) {
		api_v2.RegisterCollectorServiceServer(s, handler)
	})
	defer s.Stop()
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	//lint:ignore SA5001 don't care about errors
	defer conn.Close()
	require.NoError(t, err)

	rep := NewReporter(conn, &reporter.Options{}, zap.NewNop())

	tm := time.Unix(158, 0)
	a := tm.Unix() * 1000 * 1000
	tests := []struct {
		in       *zipkincore.Span
		expected model.Batch
		err      string
	}{
		{in: &zipkincore.Span{}, err: "cannot find service name in Zipkin span [traceID=0, spanID=0]"},
		{in: &zipkincore.Span{Name: "jonatan", TraceID: 1, ID: 2, Timestamp: &a, Annotations: []*zipkincore.Annotation{{Value: zipkincore.CLIENT_SEND, Host: &zipkincore.Endpoint{ServiceName: "spring"}}}},
			expected: model.Batch{
				Spans: []*model.Span{{TraceID: model.NewTraceID(0, 1), SpanID: model.NewSpanID(2), OperationName: "jonatan", Duration: time.Microsecond * 1,
					Tags:    model.KeyValues{{Key: "span.kind", VStr: "client", VType: model.StringType}},
					Process: &model.Process{ServiceName: "spring"}, StartTime: tm.UTC()}}}},
	}
	for _, test := range tests {
		err = rep.EmitZipkinBatch([]*zipkincore.Span{test.in})
		if test.err != "" {
			assert.EqualError(t, err, test.err)
		} else {
			assert.Equal(t, 1, len(handler.requests))
			assert.Equal(t, test.expected, handler.requests[0].GetBatch())
		}
	}
}

func TestReporter_EmitBatch(t *testing.T) {
	handler := &mockSpanHandler{}
	s, addr := initializeGRPCTestServer(t, func(s *grpc.Server) {
		api_v2.RegisterCollectorServiceServer(s, handler)
	})
	defer s.Stop()
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	//lint:ignore SA5001 don't care about errors
	defer conn.Close()
	require.NoError(t, err)
	rep := NewReporter(conn, &reporter.Options{}, zap.NewNop())

	tm := time.Unix(158, 0)
	tests := []struct {
		in       *jThrift.Batch
		expected model.Batch
		err      string
	}{
		{in: &jThrift.Batch{Process: &jThrift.Process{ServiceName: "node"}, Spans: []*jThrift.Span{{OperationName: "foo", StartTime: int64(model.TimeAsEpochMicroseconds(tm))}}},
			expected: model.Batch{Process: &model.Process{ServiceName: "node"}, Spans: []*model.Span{{OperationName: "foo", StartTime: tm.UTC()}}}},
	}
	for _, test := range tests {
		err = rep.EmitBatch(test.in)
		if test.err != "" {
			assert.EqualError(t, err, test.err)
		} else {
			assert.Equal(t, 1, len(handler.requests))
			assert.Equal(t, test.expected, handler.requests[0].GetBatch())
		}
	}
}

func TestReporter_SendFailure(t *testing.T) {
	conn, err := grpc.Dial("", grpc.WithInsecure())
	require.NoError(t, err)
	rep := NewReporter(conn, &reporter.Options{}, zap.NewNop())
	err = rep.send(nil, nil)
	assert.EqualError(t, err, "rpc error: code = Unavailable desc = all SubConns are in TransientFailure, latest connection error: connection error: desc = \"transport: Error while dialing dial tcp: missing address\"")
}

func TestReporter_AddProcessTags_EmptyTags(t *testing.T) {
	tags := map[string]string{}
	spans := []*model.Span{{TraceID: model.NewTraceID(0, 1), SpanID: model.NewSpanID(2), OperationName: "jonatan"}}
	actualSpans, _ := common.AddProcessTags(spans, nil, model.KeyValueFromMap(tags))
	assert.Equal(t, spans, actualSpans)
}

func TestReporter_AddProcessTags_ZipkinBatch(t *testing.T) {
	tags := map[string]string{"key": "value"}
	spans := []*model.Span{{TraceID: model.NewTraceID(0, 1), SpanID: model.NewSpanID(2), OperationName: "jonatan", Process: &model.Process{ServiceName: "spring"}}}

	expectedSpans := []*model.Span{
		{
			TraceID:       model.NewTraceID(0, 1),
			SpanID:        model.NewSpanID(2),
			OperationName: "jonatan",
			Process:       &model.Process{ServiceName: "spring", Tags: []model.KeyValue{model.String("key", "value")}},
		},
	}
	actualSpans, _ := common.AddProcessTags(spans, nil, model.KeyValueFromMap(tags))

	assert.Equal(t, expectedSpans, actualSpans)
}

func TestReporter_AddProcessTags_JaegerBatch(t *testing.T) {
	tags := map[string]string{"key": "value"}
	spans := []*model.Span{{TraceID: model.NewTraceID(0, 1), SpanID: model.NewSpanID(2), OperationName: "jonatan"}}
	process := &model.Process{ServiceName: "spring"}

	expectedProcess := &model.Process{ServiceName: "spring", Tags: []model.KeyValue{model.String("key", "value")}}
	_, actualProcess := common.AddProcessTags(spans, process, model.KeyValueFromMap(tags))

	assert.Equal(t, expectedProcess, actualProcess)
}

func TestReporter_MakeModelKeyValue(t *testing.T) {
	expectedTags := []model.KeyValue{model.String("key", "value")}
	stringTags := map[string]string{"key": "value"}
	actualTags := model.KeyValueFromMap(stringTags)

	assert.Equal(t, expectedTags, actualTags)
}

func TestRetryableLogic(t *testing.T) {
	assert := assert.New(t)
	acceptable := []codes.Code{
		codes.DeadlineExceeded,
		codes.Unavailable,
		codes.Unknown,
	}
	for _, a := range acceptable {
		s := status.Error(a, "Retryable")
		gerr := gRPCReporterError{s}
		assert.True(gerr.IsRetryable())
	}

	unacceptable := status.Error(codes.InvalidArgument, "NotRetryable")
	gerr := gRPCReporterError{unacceptable}
	assert.False(gerr.IsRetryable())
}

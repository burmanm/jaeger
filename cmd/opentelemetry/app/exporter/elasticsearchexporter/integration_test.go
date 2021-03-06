// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
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

// +build integration

package elasticsearchexporter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/opentelemetry/app/internal/esclient"
	"github.com/jaegertracing/jaeger/cmd/opentelemetry/app/internal/reader/es/esdependencyreader"
	"github.com/jaegertracing/jaeger/cmd/opentelemetry/app/internal/reader/es/esspanreader"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/es/config"
	"github.com/jaegertracing/jaeger/pkg/testutils"
	"github.com/jaegertracing/jaeger/plugin/storage/es"
	"github.com/jaegertracing/jaeger/plugin/storage/es/spanstore/dbmodel"
	"github.com/jaegertracing/jaeger/plugin/storage/integration"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const (
	host            = "0.0.0.0"
	esPort          = "9200"
	esHostPort      = host + ":" + esPort
	esURL           = "http://" + esHostPort
	indexPrefix     = "integration-test"
	tagKeyDeDotChar = "@"
	maxSpanAge      = time.Hour * 72
)

type IntegrationTest struct {
	integration.StorageIntegration

	logger *zap.Logger
}

type storageWrapper struct {
	writer *esSpanWriter
}

var _ spanstore.Writer = (*storageWrapper)(nil)

func (s storageWrapper) WriteSpan(ctx context.Context, span *model.Span) error {
	// This fails because there is no binary tag type in OTEL and also OTEL span's status code is always created
	//traces := jaegertranslator.ProtoBatchesToInternalTraces([]*model.Batch{{Process: span.Process, Spans: []*model.Span{span}}})
	//_, err := s.writer.WriteTraces(context.Background(), traces)
	converter := dbmodel.FromDomain{}
	dbSpan := converter.FromDomainEmbedProcess(span)
	_, err := s.writer.writeSpans(ctx, []*dbmodel.Span{dbSpan})
	return err
}

func (s *IntegrationTest) initializeES(allTagsAsFields bool) error {
	s.logger, _ = testutils.NewLogger()

	s.initSpanstore(allTagsAsFields)
	s.CleanUp = func() error {
		return s.esCleanUp()
	}
	s.Refresh = s.esRefresh
	s.esCleanUp()
	// TODO: remove this flag after ES support returning spanKind when get operations
	s.NotSupportSpanKindWithOperation = true
	return nil
}

func (s *IntegrationTest) esCleanUp() error {
	request, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/*", esURL), strings.NewReader(""))
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	return response.Body.Close()
}

func (s *IntegrationTest) initSpanstore(allTagsAsFields bool) error {
	cfg := config.Configuration{
		Servers:     []string{esURL},
		IndexPrefix: indexPrefix,
		Tags: config.TagsAsFields{
			AllAsFields: allTagsAsFields,
		},
	}
	w, err := newEsSpanWriter(cfg, s.logger, false)
	if err != nil {
		return err
	}
	esVersion := uint(w.esClientVersion())
	spanMapping, serviceMapping := es.GetSpanServiceMappings(5, 1, esVersion)
	err = w.CreateTemplates(context.Background(), spanMapping, serviceMapping)
	if err != nil {
		return err
	}
	s.SpanWriter = storageWrapper{
		writer: w,
	}

	elasticsearchClient, err := esclient.NewElasticsearchClient(cfg, s.logger)
	if err != nil {
		return err
	}
	reader := esspanreader.NewEsSpanReader(elasticsearchClient, s.logger, esspanreader.Config{
		IndexPrefix:       indexPrefix,
		TagDotReplacement: tagKeyDeDotChar,
		MaxSpanAge:        maxSpanAge,
		MaxNumSpans:       10_000,
	})
	s.SpanReader = reader

	depMapping := es.GetDependenciesMappings(1, 0, esVersion)
	depStore := esdependencyreader.NewDependencyStore(elasticsearchClient, s.logger, indexPrefix)
	if err := depStore.CreateTemplates(depMapping); err != nil {
		return nil
	}
	s.DependencyReader = depStore
	s.DependencyWriter = depStore
	return nil
}

func (s *IntegrationTest) esRefresh() error {
	response, err := http.Post(fmt.Sprintf("%s/_refresh", esURL), "application/json", strings.NewReader(""))
	if err != nil {
		return err
	}
	return response.Body.Close()
}

func healthCheck() error {
	for i := 0; i < 200; i++ {
		if _, err := http.Get(esURL); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("elastic search is not ready")
}

func testElasticsearchStorage(t *testing.T, allTagsAsFields bool) {
	if err := healthCheck(); err != nil {
		t.Fatal(err)
	}
	s := &IntegrationTest{
		StorageIntegration: integration.StorageIntegration{
			FixturesPath: "../../../../../plugin/storage/integration",
		},
	}
	require.NoError(t, s.initializeES(allTagsAsFields))
	s.Fixtures = integration.LoadAndParseQueryTestCases(t, "../../../../../plugin/storage/integration/fixtures/queries_es.json")
	s.IntegrationTestAll(t)
}

func TestElasticsearchStorage(t *testing.T) {
	testElasticsearchStorage(t, false)
}

func TestElasticsearchStorage_AllTagsAsObjectFields(t *testing.T) {
	testElasticsearchStorage(t, true)
}

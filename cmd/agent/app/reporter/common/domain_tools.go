package common

import "github.com/jaegertracing/jaeger/model"

// AddProcessTags appends jaeger tags for the agent to every span it sends to the collector.
func AddProcessTags(spans []*model.Span, process *model.Process, agentTags []model.KeyValue) ([]*model.Span, *model.Process) {
	if len(agentTags) == 0 {
		return spans, process
	}
	if process != nil {
		process.Tags = append(process.Tags, agentTags...)
	}
	for _, span := range spans {
		if span.Process != nil {
			span.Process.Tags = append(span.Process.Tags, agentTags...)
		}
	}
	return spans, process
}

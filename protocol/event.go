package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// TaskEvent represents a task event message
type event struct {
	Hostname  string `json:"hostname"`
	Type      string `json:"type"`
	Clock     int    `json:"clock"`
	Timestamp int64  `json:"timestamp"`
	UTCOffset int    `json:"utcoffset"`
	Pid       int    `json:"pid"`
}

type outboundMessageEvent struct {
	Body            string                     `json:"body"`
	ContentEncoding string                     `json:"content-encoding"`
	ContentType     string                     `json:"content-type"`
	Header          outboundMessageEventHeader `json:"headers"`
	Property        outboundMessageProperty    `json:"properties"`
}

type outboundMessageEventHeader struct {
	Hostname string `json:"hostname"`
}

func (r *SerializerRegistry) Event(eventType, queue, routingKey string, obj interface{}) ([]byte, error) {
	now := time.Now()
	_, offset := now.Zone()

	base := &event{
		Hostname:  r.host,
		Type:      eventType,
		Clock:     r.Clock(),
		Timestamp: now.Unix(),
		UTCOffset: -offset / 3600,
		Pid:       r.pid,
	}

	body := map[string]json.RawMessage{}

	if err := toMap(obj, &body); err != nil {
		return nil, err
	}

	if err := toMap(base, &body); err != nil {
		return nil, err
	}

	// Events always use application/json
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("json encode: %w", err)
	}

	if strings.HasPrefix(eventType, "task-") {
		payload = append([]byte{'['}, payload...)
		payload = append(payload, ']')
	}

	m := outboundMessageEvent{
		Body:            base64.StdEncoding.EncodeToString(payload),
		ContentEncoding: r.encoding["application/json"],
		ContentType:     "application/json",
		Header: outboundMessageEventHeader{
			Hostname: r.host,
		},
		Property: outboundMessageProperty{
			BodyEncoding: "base64",
			DeliveryInfo: outboundMessageDeliveryInfo{
				Exchange:   queue,
				RoutingKey: routingKey,
			},
			DeliveryMode: 1,
			DeliveryTag:  r.uuid4(),
		},
	}

	return json.Marshal(&m)
}

func toMap(obj interface{}, target *map[string]json.RawMessage) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, target)
}

func (r *SerializerRegistry) Clock() int {
	r.Lock()

	defer r.Unlock()

	r.clock++

	return r.clock
}

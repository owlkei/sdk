package ws

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"go.owlkei.com/sdk/internal/test"
)

type vector struct {
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Input struct {
		Text   *string `json:"text"`
		Binary *struct {
			Envelope   string `json:"envelope"`
			DataBase64 string `json:"dataBase64"`
		} `json:"binary"`
		FrameBase64 *string `json:"frameBase64"`
	} `json:"input"`
	Want map[string]json.RawMessage `json:"want"`
}

func parseVectors(t *testing.T) []vector {
	t.Helper()
	paths, err := filepath.Glob("../../spec/ws/v1/vectors/parse-*.json")
	if err != nil || len(paths) < 30 {
		t.Fatalf("the spec vectors are missing: %d found, %v", len(paths), err)
	}
	out := make([]vector, 0, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var v vector
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatal(p, err)
		}
		out = append(out, v)
	}
	return out
}

func wantString(v vector, key string) string {
	var s string
	_ = json.Unmarshal(v.Want[key], &s)
	return s
}

func TestVectors_ValidEnvelopesDecode(t *testing.T) {
	for _, v := range parseVectors(t) {
		if _, isError := v.Want["error"]; isError || v.Input.Text == nil {
			continue
		}
		in, err := decodeText([]byte(*v.Input.Text))
		if err != nil {
			t.Error(test.DiffMessage(err, nil, v.Name))
			continue
		}
		if in.envelope.Type != wantString(v, "type") || in.envelope.ID != wantString(v, "id") || in.envelope.Topic != wantString(v, "topic") {
			t.Error(test.DiffMessage(in.envelope, string(v.Want["type"])+" "+string(v.Want["id"])+" "+string(v.Want["topic"]), v.Name))
		}
		if want, ok := v.Want["data"]; ok {
			var got, exp any
			_ = json.Unmarshal(in.envelope.Data, &got)
			_ = json.Unmarshal(want, &exp)
			if !reflect.DeepEqual(got, exp) {
				t.Error(test.DiffMessage(got, exp, v.Name+": data"))
			}
		}
		if id := wantString(v, "id"); id != "" && !isID(id) {
			t.Error(test.DiffMessage(id, "a valid id", v.Name))
		}
		if topic := wantString(v, "topic"); topic != "" && !IsTopic(topic) {
			t.Error(test.DiffMessage(topic, "a valid topic", v.Name))
		}
	}
}

func TestVectors_BinaryRoundTrip(t *testing.T) {
	for _, v := range parseVectors(t) {
		if _, isError := v.Want["error"]; isError || v.Input.Binary == nil {
			continue
		}
		var e envelope
		if err := json.Unmarshal([]byte(v.Input.Binary.Envelope), &e); err != nil {
			t.Fatal(v.Name, err)
		}
		data, _ := base64.StdEncoding.DecodeString(v.Input.Binary.DataBase64)
		frame, err := encodeBinary(e, data)
		if err != nil {
			t.Fatal(v.Name, err)
		}
		in, err := decodeBinary(frame)
		want, _ := base64.StdEncoding.DecodeString(wantString(v, "dataBase64"))
		if err != nil || in.envelope.Type != e.Type || in.envelope.ID != e.ID || !reflect.DeepEqual(append([]byte{}, in.binary...), append([]byte{}, want...)) {
			t.Error(test.DiffMessage(in, want, v.Name))
		}
	}
}

func TestVectors_MalformedBinaryFramesAreRefused(t *testing.T) {
	n := 0
	for _, v := range parseVectors(t) {
		if v.Input.FrameBase64 == nil {
			continue
		}
		n++
		frame, _ := base64.StdEncoding.DecodeString(*v.Input.FrameBase64)
		if _, err := decodeBinary(frame); err == nil {
			t.Error(test.DiffMessage(nil, "an error", v.Name))
		}
	}
	if n == 0 {
		t.Error("no malformed frame vector")
	}
}

func TestVectors_InvalidTopicsAndIDs(t *testing.T) {
	topics := []string{"rooms..42", "rooms.", "rooms.*", strings.Repeat("a", 256), strings.Repeat("s.", 16) + "s", "phòng"}
	for _, topic := range topics {
		if IsTopic(topic) {
			t.Error(test.DiffMessage(topic, "not a topic", "V5"))
		}
	}
	for _, id := range []string{"", strings.Repeat("x", 65), "a b", "a~b"} {
		if isID(id) {
			t.Error(test.DiffMessage(id, "not an id", "V5"))
		}
	}
}

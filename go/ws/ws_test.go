package ws

import (
	"testing"

	"go.owlkei.com/sdk/internal/test"
)

func TestProtocol_NamesTheWireProtocol(t *testing.T) {
	if Protocol != "owlkei.v1" {
		t.Error(test.DiffMessage(Protocol, "owlkei.v1", "the subprotocol of the wire protocol v1"))
	}
}

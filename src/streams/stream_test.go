package streams

import (
	"bytes"
	"testing"
)

func Test_message(t *testing.T) {
	msg := Message{Bucket: "abc", Key: "abc", Data: []byte{1, 2, 3}}
	serializedMsg := []byte{11, 0, 0, 0, 3, 'a', 'b', 'c', 3, 'a', 'b', 'c', 1, 2, 3}
	serialized := msg.Serialize()
	if !bytes.Equal(serialized, serializedMsg) {
		t.Errorf("message serialization failed, expected %v, result %v", serializedMsg, serialized)
	}

}

package streams

import (
	"bytes"
	"testing"
)

func Test_message(t *testing.T) {
	msg := Message{Bucket: "abc", Key: "abc", Data: []byte{1, 2, 3}}
	serializedMsg := []byte{11, 0, 0, 0, 3, 'a', 'b', 'c', 3, 'a', 'b', 'c', 1, 2, 3}
	serialized := msg.Serialize(true)
	if !bytes.Equal(serialized, serializedMsg) {
		t.Errorf("message serialization failed, expected %+v, result %+v", serializedMsg, serialized)
	}
	unserializedMsg, err := GetMessage(serialized[4:]) //skip message length
	if err != nil {
		t.Fatalf("deserialization of serialized message failed. err: %s", err)
	}
	if unserializedMsg.Bucket != msg.Bucket {
		t.Errorf("deserialization of serialized message failed. bucket field is different. expected %s, found %s", msg.Bucket, unserializedMsg.Bucket)
	}
	if !bytes.Equal(unserializedMsg.Data, msg.Data) {
		t.Errorf("deserialization of serialized message failed. data field is different. expected %+v, found %+v", msg.Data, unserializedMsg.Data)
	}
	if unserializedMsg.Key != msg.Key {
		t.Errorf("deserialization of serialized message failed. key field is different. expected %s (%x), found %s (%x)", msg.Key, msg.Key, unserializedMsg.Key, unserializedMsg.Key)
	}
}

func Test_message_empty_key(t *testing.T) {
	msg := Message{Bucket: "abc", Key: "", Data: []byte{1, 2, 3}}
	serializedMsg := []byte{8, 0, 0, 0, 3, 'a', 'b', 'c', 0, 1, 2, 3}
	serialized := msg.Serialize(true)
	if !bytes.Equal(serialized, serializedMsg) {
		t.Errorf("message serialization failed, expected %+v, result %+v", serializedMsg, serialized)
	}
	unserializedMsg, err := GetMessage(serialized[4:]) //skip message length
	if err != nil {
		t.Fatalf("deserialization of serialized message failed. err: %s", err)
	}
	if unserializedMsg.Bucket != msg.Bucket {
		t.Errorf("deserialization of serialized message failed. bucket field is different. . expected %s (%x), found %s (%x)", msg.Bucket, msg.Bucket, unserializedMsg.Bucket, unserializedMsg.Bucket)
	}
	if !bytes.Equal(unserializedMsg.Data, msg.Data) {
		t.Errorf("deserialization of serialized message failed. data field is different. expected %+v, found %+v", msg.Data, unserializedMsg.Data)
	}
	if unserializedMsg.Key != msg.Key {
		t.Errorf("deserialization of serialized message failed. key field is different. expected %s (%x), found %s (%x)", msg.Key, msg.Key, unserializedMsg.Key, unserializedMsg.Key)
	}
}

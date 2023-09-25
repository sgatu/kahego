package stream

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/datastructures"
)

/*
FileStream used to write data to file
*/
type FileStream struct {
	path         string
	fileName     string
	file         *os.File
	queue        datastructures.Queue[*Message]
	writtenBytes int32
	rotateLength int32
	slice        float32
	hasError     bool
	lastErr      error
}

func (stream *FileStream) Push(msg *Message) error {
	stream.queue.Push(&datastructures.Node[*Message]{Value: msg})
	return nil
}
func (stream *FileStream) rotateFile() error {
	if stream.writtenBytes >= stream.rotateLength || stream.file == nil {
		stream.writtenBytes = 0
		if stream.file != nil {
			stream.file.Sync()
			stream.file.Close()
		}
		fullPath := stream.path + stream.fileName + "-" + fmt.Sprintf("%d", time.Now().Unix())
		file, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			stream.hasError = true
			stream.lastErr = err
			return err
		}
		stream.file = file
	}
	return nil

}
func (stream *FileStream) flush() error {
	err := stream.rotateFile()
	if err != nil {
		return err
	}
	length := stream.queue.Len()
	for i := 0; i < int(length); i++ {
		node, err := stream.queue.Pop()
		if err == nil {
			serializedData := node.Value.Serialize()
			if _, err := stream.file.Write(serializedData); err != nil {
				stream.file.Sync()
				return err
			}
			stream.writtenBytes += int32(len(serializedData))
		} else {
			break
		}
	}
	return stream.file.Sync()
}
func (stream *FileStream) Flush() error {
	fmt.Printf("Flushing fileStream with %d pending messages\n", stream.queue.Len())
	err := stream.flush()
	if err != nil {
		stream.lastErr = err
		stream.hasError = true
	}
	return err
}
func (stream *FileStream) Len() uint32 {
	return stream.queue.Len()
}
func (stream *FileStream) Close() error {
	if stream.file != nil {
		stream.file.Close()
	}
	return nil
}
func (stream *FileStream) Init() error {
	stream.hasError = false
	stream.lastErr = nil
	err := stream.rotateFile()
	if err != nil {
		return err
	}
	return nil
}
func (stream *FileStream) HasError() bool {
	return stream.hasError
}
func (stream *FileStream) GetError() error {
	return stream.lastErr
}
func (stream *FileStream) GetQueue() *datastructures.Queue[*Message] {
	return &stream.queue
}
func (stream *FileStream) GetSlice() float32 {
	return stream.slice
}

func getFileStream(streamConfig config.StreamConfig) (*FileStream, error) {
	path, ok := streamConfig.Settings["path"]
	if !ok {
		return nil, errors.New("no path defined for fileStream")
	}
	rotateLengthStr, ok := streamConfig.Settings["sizeRotate"]
	var rotateLength int32 = 1024 * 1024 * 100 //100 MB default file size
	if ok {
		fmt.Println("Found sizeRotate in config", rotateLengthStr)
		i, err := strconv.ParseInt(rotateLengthStr, 10, 32)
		if err == nil {
			fmt.Println("Parsed sizeRotate to", i)
			rotateLength = int32(i)
		} else {
			fmt.Println(err)
		}
	}
	fs := &FileStream{
		path:         path,
		file:         nil,
		writtenBytes: 0,
		rotateLength: rotateLength,
		fileName:     streamConfig.Key,
		hasError:     false,
		slice:        streamConfig.Slice,
	}
	return fs, nil
}

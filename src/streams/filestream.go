package streams

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/inhies/go-bytesize"
	"sgatu.com/kahego/src/config"
	"sgatu.com/kahego/src/datastructures"
)

/*
FileStream used to write data to file
*/
type FileStream struct {
	path             string
	fileNameTemplate string
	bucketId         string
	file             *os.File
	queue            datastructures.Queue[*Message]
	writtenBytes     int32
	rotateLength     int32
	slice            float32
	hasError         bool
	lastErr          error
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
		fileName := strings.ReplaceAll(stream.fileNameTemplate, "{ts}", fmt.Sprintf("%d", time.Now().Unix()))
		fileName = strings.ReplaceAll(fileName, "{bucket}", stream.bucketId)
		fullPath := stream.path + fileName
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
func isLastCharNumeric(s string) bool {
	if len(s) == 0 {
		return false
	}
	lastChar := rune(s[len(s)-1])
	return unicode.IsDigit(lastChar)
}

/*
Factory method
*/
func getFileStream(streamConfig config.StreamConfig, bucket string) (*FileStream, error) {
	path, ok := streamConfig.Settings["path"]
	if !ok {
		return nil, errors.New("no path defined for fileStream")
	}
	rotateLengthStr, ok := streamConfig.Settings["sizeRotate"]
	var rotateLength int32 = 1024 * 1024 * 100 //100 MB default file size
	var fileNameTemplate string = bucket
	if ok {
		if isLastCharNumeric(rotateLengthStr) {
			rotateLengthStr += "B"
		}
		i, err := bytesize.Parse(rotateLengthStr)
		if err == nil {
			fmt.Println("Parsed sizeRotate to", i)
			rotateLength = int32(i)
		} else {
			fmt.Println(err)
		}
	}
	fileNameTemplateStr, ok := streamConfig.Settings["fileNameTemplate"]
	if ok {
		fileNameTemplate = fileNameTemplateStr
	}
	fs := &FileStream{
		path:             path,
		file:             nil,
		writtenBytes:     0,
		rotateLength:     rotateLength,
		fileNameTemplate: fileNameTemplate,
		bucketId:         bucket,
		hasError:         false,
		slice:            streamConfig.Slice,
	}
	return fs, nil
}

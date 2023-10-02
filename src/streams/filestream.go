package streams

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
	filesPaths       datastructures.Queue[string]
	writtenBytes     int32
	rotateLength     int32
	maxFiles         uint32
	slice            float32
	hasError         bool
	lastErr          error
}

func (stream *FileStream) Push(msg *Message) error {
	stream.queue.Push(&datastructures.Node[*Message]{Value: msg})
	return nil
}
func (stream *FileStream) getFilesPattern() (string, string) {
	fileName := strings.ReplaceAll(stream.fileNameTemplate, "{ts}", "*")
	fileName = strings.ReplaceAll(fileName, "{bucket}", stream.bucketId)
	fileName = strings.ReplaceAll(fileName, "{PS}", string(os.PathSeparator))
	hostname, err := os.Hostname()
	if err == nil {
		fileName = strings.ReplaceAll(fileName, "{hostname}", hostname)
	}
	fileDirs := strings.Split(fileName, string(os.PathSeparator))
	fullPath := stream.path
	if fullPath[len(fullPath)-1] == os.PathSeparator {
		fullPath = strings.TrimRight(fullPath, string(os.PathSeparator))
	}
	for i := 0; i < len(fileDirs); i++ {
		if i == len(fileDirs)-1 {
			fileName = fileDirs[i]
		} else {
			fullPath = fullPath + string(os.PathSeparator) + fileDirs[i]
		}
	}
	return fullPath, fileName
}
func (stream *FileStream) getNextFileName() (string, string) {
	fileName := strings.ReplaceAll(stream.fileNameTemplate, "{ts}", fmt.Sprintf("%d", time.Now().Unix()))
	fileName = strings.ReplaceAll(fileName, "{bucket}", stream.bucketId)
	fileName = strings.ReplaceAll(fileName, "{PS}", string(os.PathSeparator))
	hostname, err := os.Hostname()
	if err == nil {
		fileName = strings.ReplaceAll(fileName, "{hostname}", hostname)
	} else {
		fmt.Println(err)
	}
	fileDirs := strings.Split(fileName, string(os.PathSeparator))
	fullPath := stream.path
	if fullPath[len(fullPath)-1] == os.PathSeparator {
		fullPath = strings.TrimRight(fullPath, string(os.PathSeparator))
	}
	for i := 0; i < len(fileDirs); i++ {
		if i == len(fileDirs)-1 {
			fileName = fileDirs[i]
		} else {
			fullPath = fullPath + string(os.PathSeparator) + fileDirs[i]
		}
	}
	return fullPath, fileName
}
func (stream *FileStream) rotateFile() error {
	if stream.writtenBytes >= stream.rotateLength || stream.file == nil {
		stream.writtenBytes = 0
		if stream.file != nil {
			stream.file.Sync()
			stream.file.Close()
		}
		for stream.filesPaths.Len() >= stream.maxFiles {
			path, _ := stream.filesPaths.Pop()
			os.Remove(path.Value)
		}
		filePath, fileName := stream.getNextFileName()
		os.MkdirAll(filePath, 0777)
		fullPath := filePath + string(os.PathSeparator) + fileName
		stream.filesPaths.Push(&datastructures.Node[string]{Value: fullPath})
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
	length := stream.queue.Len()
	for i := 0; i < int(length); i++ {
		if i%100 == 0 || i == 0 {
			err := stream.rotateFile()
			if err != nil {
				return err
			}
		}
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
	dir, filePattern := stream.getFilesPattern()
	files, rerr := os.ReadDir(dir)
	if rerr == nil {
		for _, file := range files {
			if _, err := filepath.Match(filePattern, file.Name()); err == nil {
				fmt.Println("Found existing file at ", dir+string(os.PathSeparator)+file.Name())
				stream.filesPaths.Push(&datastructures.Node[string]{Value: dir + string(os.PathSeparator) + file.Name()})
			}
		}
	}
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
	var maxFiles uint32 = 1024
	if maxFilesStr, ok := streamConfig.Settings["maxFiles"]; ok {
		maxFilesParsed, err := strconv.ParseUint(maxFilesStr, 10, 32)
		if err == nil {
			maxFiles = uint32(maxFilesParsed)
		}
		if maxFiles < 1 {
			maxFiles = 1
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
		maxFiles:         maxFiles,
	}
	return fs, nil
}

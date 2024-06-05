package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"time"
)

const (
	Version     = 0
	ChunkSize   = 2 * 1024 // 1MB
	PathLength  = 512
	Retransmits = 4
	ServerPort  = ":9000"
	ChunkType   = 0
	PathType    = 1
	BackoffTime = 50000 * time.Nanosecond
)

type Chunk struct {
	Version  uint8
	Type     uint8
	Size     uint16
	Index    uint64
	PathId   uint32
	Checksum uint32
	Data     []byte
}

var pathMap map[uint32][]byte = map[uint32][]byte{}
var pathMapIndex uint32 = 0

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: server <file_path>")
		os.Exit(1)
	}

	filePath := make([]byte, PathLength)
	copy(filePath[0:len(os.Args[1])], os.Args[1])

	// Read the file
	file, err := os.Open(string(os.Args[1]))
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Setup UDP connection
	conn, err := net.Dial("udp", "localhost"+ServerPort)
	if err != nil {
		log.Fatalf("Failed to connect to UDP server: %v", err)
	}
	defer conn.Close()

	buffer := make([]byte, ChunkSize)
	var index uint64 = 0

	pathId := sendPath(conn, filePath)

	for range Retransmits {
		for {
			bytesRead, err := file.Read(buffer)
			if err != nil && err != io.EOF {
				log.Fatalf("Failed to read file: %v", err)
			}
			if bytesRead == 0 {
				break
			}

			chunk := Chunk{
				Index:  index,
				Size:   uint16(bytesRead),
				PathId: pathId,
			}

			chunk.Data = make([]byte, ChunkSize)
			copy(chunk.Data, buffer[:bytesRead])

			sendChunk(conn, chunk)
			time.Sleep(BackoffTime)

			index += uint64(bytesRead)
		}
		index = 0
		file.Seek(0, 0)
	}
}

func encodeChunk(chunk Chunk) []byte {
	if len(chunk.Data) == 0 {
		return []byte{}
	}
	chunk.Version = Version
	chunk.Type = ChunkType
	chunk.Checksum = crc32.ChecksumIEEE(chunk.Data)
	data := make([]byte, PathLength+ChunkSize+32)
	data[0] = chunk.Version
	data[1] = chunk.Type
	binary.LittleEndian.PutUint16(data[2:4], chunk.Size)
	binary.LittleEndian.PutUint64(data[4:20], chunk.Index)
	binary.LittleEndian.PutUint32(data[20:24], chunk.PathId)
	binary.LittleEndian.PutUint32(data[24:28], chunk.Checksum)
	copy(data[28:28+ChunkSize], chunk.Data)
	return data
}

func decodeChunk(b []byte) Chunk {
	chunk := Chunk{
		Version:  b[0],
		Type:     b[1],
		Size:     binary.LittleEndian.Uint16(b[2:4]),
		Index:    binary.LittleEndian.Uint64(b[4:20]),
		PathId:   binary.LittleEndian.Uint32(b[20:24]),
		Checksum: binary.LittleEndian.Uint32(b[24:28]),
		Data:     b[28 : 28+binary.LittleEndian.Uint16(b[2:4])],
	}
	return chunk
}
func validateChunk(chunk Chunk) bool {
	return crc32.ChecksumIEEE(chunk.Data) == chunk.Checksum
}

func sendChunk(conn net.Conn, chunk Chunk) {
	_, err := conn.Write(encodeChunk(chunk))
	if err != nil {
		log.Printf("Failed to send chunk: %v", err)
	}
}

func sendPath(conn net.Conn, path []byte) uint32 {
	pathMapIndex++
	pathMap[pathMapIndex] = path
	pathChunk := Chunk{
		Version: Version,
		Type:    PathType,
		Size:    uint16(len(path)),
		PathId:  pathMapIndex,
		Data:    path,
	}
	for range Retransmits {
		_, err := conn.Write(encodeChunk(pathChunk))
		if err != nil {
			log.Printf("Failed to send path %s: %v", path, err)
		}
		time.Sleep(BackoffTime)
	}
	return pathMapIndex
}

package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"log"
	"net"
	"os"
  "time"
)

const (
	ChunkSize   = 2 * 1024 // 1MB
	PathLength  = 256
	Retransmits = 8
	ServerPort  = ":9000"
)

var crcTable = crc64.MakeTable(crc64.ISO)

type Chunk struct {
	Version  uint64
	Index    uint64
	Size     uint64
	Checksum uint64
	Path     []byte
	Data     []byte
}

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
				Index: index,
				Size:  uint64(bytesRead),
				Path:  []byte(filePath)[0:PathLength],
			}

			chunk.Data = make([]byte, ChunkSize)
			copy(chunk.Data, buffer[:bytesRead])

			sendChunk(conn, chunk)
      time.Sleep(50000 * time.Nanosecond)

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
	chunk.Checksum = crc64.Checksum(chunk.Data, crcTable)
	data := make([]byte, PathLength+ChunkSize+32)
	binary.LittleEndian.PutUint64(data[0:8], chunk.Version)
	binary.LittleEndian.PutUint64(data[8:16], chunk.Index)
	binary.LittleEndian.PutUint64(data[16:24], chunk.Size)
	binary.LittleEndian.PutUint64(data[24:32], chunk.Checksum)
	copy(data[32:PathLength+32], chunk.Path)
	copy(data[PathLength+32:PathLength+32+ChunkSize], chunk.Data)
	return data
}

func decodeChunk(b []byte) Chunk {
	chunk := Chunk{
		Version:  binary.LittleEndian.Uint64(b[0:8]),
		Index:    binary.LittleEndian.Uint64(b[8:16]),
		Size:     binary.LittleEndian.Uint64(b[16:24]),
		Checksum: binary.LittleEndian.Uint64(b[24:32]),
		Data:     b[PathLength+32 : PathLength+32+ChunkSize],
	}
  for pathIndex := range PathLength {
    if b[32+pathIndex] == 0 {
      break
    }
    chunk.Path = append(chunk.Path, b[32+pathIndex])
  }
	return chunk
}
func validateChunk(chunk Chunk) bool {
	return crc64.Checksum(chunk.Data, crcTable) == chunk.Checksum
}

func sendChunk(conn net.Conn, chunk Chunk) {
	_, err := conn.Write(encodeChunk(chunk))
	if err != nil {
		log.Printf("Failed to send chunk: %v", err)
	}
}

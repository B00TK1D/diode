package main

import (
  "hash/crc64"
	"encoding/binary"
	"log"
	"net"
	"os"
//  "github.com/vivint/infectious"
)

const (
	ChunkSize   = 2 * 1024 // 1MB
	PathLength  = 256
	Retransmits = 32
	ServerPort  = ":9000"
)

var crcTable = crc64.MakeTable(crc64.ISO)

var totalChunks = 0

type Chunk struct {
	Version  uint64
	Index    uint64
	Size     uint64
	Checksum uint64
	Path     []byte
	Data     []byte
}


func main() {
	// Setup UDP server
	addr, err := net.ResolveUDPAddr("udp", ServerPort)
	if err != nil {
		log.Fatalf("Failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP: %v", err)
	}
	defer conn.Close()

	buffer := make([]byte, PathLength+ChunkSize+32)

	for {
		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Failed to read from UDP: %v", err)
			continue
		}

		if n == 0 {
			continue
		}

		handleChunk(buffer[:n])
	}
}

func handleChunk(data []byte) {
  chunk := decodeChunk(data)

  //log.Printf("Receiving path %s", chunk.Path)

	writeChunk(chunk)
  totalChunks++

  if chunk.Size < ChunkSize {
    log.Printf("Finished receiving %s (total received %d chunks)", chunk.Path, totalChunks)
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


func writeChunk(chunk Chunk) {
  if !validateChunk(chunk){
    log.Printf("Corrupted chunk %d of %s", chunk.Index, chunk.Path)
    return
  }
  file, err := os.OpenFile(string(chunk.Path), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("Failed to open file: %v", err)
		return
	}
	defer file.Close()

  _, err = file.WriteAt(chunk.Data[:chunk.Size], int64(chunk.Index))
	if err != nil {
		log.Printf("Failed to write chunk: %v", err)
	}
	//log.Printf("Wrote chunk %d:%d to %s", index, index+int64(len(data)), filePath)
}

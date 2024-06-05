package main

import (
	"encoding/binary"
	"hash/crc32"
	"log"
	"net"
	"os"
	// "github.com/vivint/infectious"
)

const (
	Version     = 0
	ChunkSize   = 2 * 1024 // 1MB
	PathLength  = 512
	Retransmits = 4
	ServerPort  = ":9000"
	ChunkType   = 0
	PathType    = 1
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

var totalChunks = 0

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

	if chunk.Type == ChunkType {
		//log.Printf("Receiving path %s", chunk.Path)

    _, ok := pathMap[chunk.PathId]
    if !ok {
      log.Printf("Unknown path ID %d", chunk.PathId)
      return
    }

		writeChunk(chunk)
		totalChunks++

		if chunk.Size < ChunkSize {
			log.Printf("Finished receiving %s (total received %d chunks)", pathMap[chunk.PathId], totalChunks)
		}
    return
	}

  if chunk.Type == PathType {
    if !validateChunk(chunk) {
      return
    }
    pathMap[chunk.PathId] = chunk.Data
    log.Printf("Saving path %d = %s", chunk.PathId, chunk.Data)
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

func writeChunk(chunk Chunk) {
	if !validateChunk(chunk) {
		log.Printf("Corrupted chunk %d of %s", chunk.Index, pathMap[chunk.PathId])
		return
	}
	file, err := os.OpenFile(string(pathMap[chunk.PathId]), os.O_WRONLY|os.O_CREATE, 0644)
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

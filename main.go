package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
)

const (
	HOST        = "127.0.0.1"
	PORT        = "4242"
	PACKET_SIZE = 18 // 16 Bytes Payload + 2 Bytes CRC16
)

// SensorPayload represents the 16-byte unpacked structure
type SensorPayload struct {
	DeviceID  uint32
	Timestamp uint64
	Temp      float32
}

// Memory Pool for zero-allocation buffer recycling
var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 1024)
		return &b
	},
}

// Fast Table-based CRC16 Validation
func calculateCRC16(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		curByte := uint16(b)
		for i := 0; i < 8; i++ {
			if (crc^curByte)&0x0001 != 0 {
				crc = (crc >> 1) ^ 0xA001 // Standard Modbus CRC16 Polynomial
			} else {
				crc >>= 1
			}
			curByte >>= 1
		}
	}
	return crc
}

func main() {
	addr, err := net.ResolveUDPAddr("udp", HOST+":"+PORT)
	if err != nil {
		fmt.Printf("Error resolving address: %v\n", err)
		os.Exit(1)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("Error listening on UDP port: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Socket Receive Buffer Size 8MB-তে বাড়িয়ে দেওয়া (High Throughput-এর জন্য)
	conn.SetReadBuffer(8 * 1024 * 1024)

	fmt.Printf("🚀 Ultra-Fast Ingest Engine Live! Listening on UDP %s:%s...\n", HOST, PORT)

	for {
		// sync.Pool থেকে বাফার ধার নেওয়া
		bufPtr := bufferPool.Get().(*[]byte)
		buffer := *bufPtr

		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			bufferPool.Put(bufPtr)
			continue
		}

		// Guardrail 1: Packet Length Check
		if n != PACKET_SIZE {
			fmt.Printf("⚠️ Corrupt/Invalid packet length (%d bytes) from %s. Dropping!\n", n, remoteAddr)
			bufferPool.Put(bufPtr)
			continue
		}

		// Guardrail 2: CRC16 Verification
		payloadBytes := buffer[:16]
		receivedCRC := binary.LittleEndian.Uint16(buffer[16:18])
		calculatedCRC := calculateCRC16(payloadBytes)

		if receivedCRC != calculatedCRC {
			fmt.Printf("❌ CRC Mismatch! Received: 0x%X, Calculated: 0x%X. Dropping!\n", receivedCRC, calculatedCRC)
			bufferPool.Put(bufPtr)
			continue
		}

		// Direct Binary Unpacking (O(1) Speed)
		var payload SensorPayload
		payload.DeviceID = binary.LittleEndian.Uint32(buffer[0:4])
		payload.Timestamp = binary.LittleEndian.Uint64(buffer[4:12])
		tempBits := binary.LittleEndian.Uint32(buffer[12:16])
		payload.Temp = math.Float32frombits(tempBits)

		fmt.Printf("✅ [Valid Packet] DevID: %d | Time: %d | Temp: %.2f°C (From %s)\n",
			payload.DeviceID, payload.Timestamp, payload.Temp, remoteAddr)

		// প্রসেস শেষে বাফার পুলে ফেরত দেওয়া
		bufferPool.Put(bufPtr)
	}
}
package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	HOST        = "127.0.0.1"
	PORT        = "4242"
	PACKET_SIZE = 18
)

type SensorPayload struct {
	DeviceID  uint32
	Timestamp uint64
	Temp      float32
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 1024)
		return &b
	},
}

func calculateCRC16(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		curByte := uint16(b)
		for i := 0; i < 8; i++ {
			if (crc^curByte)&0x0001 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
			curByte >>= 1
		}
	}
	return crc
}

// Metrics Counters
var (
	totalPackets uint64
	validPackets uint64
	crcErrors    uint64
	lengthErrors uint64
)

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

	// Socket Receive Buffer Size 16MB তে বাড়িয়ে দেওয়া (High Throughput এর জন্য)
	conn.SetReadBuffer(16 * 1024 * 1024)

	fmt.Printf("🚀 Ultra-Fast Benchmarking Ingest Engine Live! UDP %s:%s...\n", HOST, PORT)

	// Live Metrics Dashboard Routine (Every 1 Second)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		var prevTotal uint64
		for range ticker.C {
			currTotal := atomic.LoadUint64(&totalPackets)
			currValid := atomic.LoadUint64(&validPackets)
			currCrcErr := atomic.LoadUint64(&crcErrors)
			currLenErr := atomic.LoadUint64(&lengthErrors)

			pps := currTotal - prevTotal
			prevTotal = currTotal

			fmt.Printf("📊 [METRICS] Speed: %d pkts/sec | Total Received: %d | Valid: %d | Bad CRC: %d | Bad Size: %d\n",
				pps, currTotal, currValid, currCrcErr, currLenErr)
		}
	}()

	for {
		bufPtr := bufferPool.Get().(*[]byte)
		buffer := *bufPtr

		n, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			bufferPool.Put(bufPtr)
			continue
		}

		atomic.AddUint64(&totalPackets, 1)

		// Guardrail 1: Packet Length Check
		if n != PACKET_SIZE {
			atomic.AddUint64(&lengthErrors, 1)
			bufferPool.Put(bufPtr)
			continue
		}

		// Guardrail 2: CRC16 Verification
		payloadBytes := buffer[:16]
		receivedCRC := binary.LittleEndian.Uint16(buffer[16:18])
		calculatedCRC := calculateCRC16(payloadBytes)

		if receivedCRC != calculatedCRC {
			atomic.AddUint64(&crcErrors, 1)
			bufferPool.Put(bufPtr)
			continue
		}

		// Direct Binary Parsing (O(1) Speed)
		var payload SensorPayload
		payload.DeviceID = binary.LittleEndian.Uint32(buffer[0:4])
		payload.Timestamp = binary.LittleEndian.Uint64(buffer[4:12])
		tempBits := binary.LittleEndian.Uint32(buffer[12:16])
		payload.Temp = math.Float32frombits(tempBits)

		atomic.AddUint64(&validPackets, 1)

		bufferPool.Put(bufPtr)
	}
}
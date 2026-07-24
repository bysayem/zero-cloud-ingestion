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
	BATCH_SIZE  = 10000                  // 10k Items per Bulk Write
	FLUSH_TIME  = 500 * time.Millisecond // 500ms Timeout
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

// 10 Lakh Queue Capacity to buffer heavy spikes
var dataQueue = make(chan SensorPayload, 1000000)

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
	totalBatches uint64
	flushedItems uint64
)

// Mock Bulk Persistence Worker
func databaseWorker() {
	batch := make([]SensorPayload, 0, BATCH_SIZE)
	ticker := time.NewTicker(FLUSH_TIME)
	defer ticker.Stop()

	for {
		select {
		case item := <-dataQueue:
			batch = append(batch, item)

			// Trigger 1: Size-based Batching (10,000 items)
			if len(batch) >= BATCH_SIZE {
				flushBatch(batch, "SIZE_TRIGGER")
				batch = batch[:0] // Reset slice without re-allocation
			}

		case <-ticker.C:
			// Trigger 2: Time-based Batching (500ms Timeout)
			if len(batch) > 0 {
				flushBatch(batch, "TIME_TRIGGER")
				batch = batch[:0] // Reset slice without re-allocation
			}
		}
	}
}

// Simulated Persistence Operation (Next step: DuckDB/SQLite Insert)
func flushBatch(batch []SensorPayload, triggerType string) {
	atomic.AddUint64(&totalBatches, 1)
	atomic.AddUint64(&flushedItems, uint64(len(batch)))

	// For debugging/benchmarking: Print batch logs only for time-triggers or large metrics
	// Real insertion logic (e.g., DuckDB Appender) will go here in the next step.
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

	conn.SetReadBuffer(16 * 1024 * 1024)

	fmt.Printf("🚀 Ultra-Fast Ingest + Micro-Batching Engine Live! UDP %s:%s...\n", HOST, PORT)

	// Start Background Persistence Worker Routine
	go databaseWorker()

	// Live Metrics Dashboard Routine (Every 1 Second)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		var prevTotal uint64
		for range ticker.C {
			currTotal := atomic.LoadUint64(&totalPackets)
			currValid := atomic.LoadUint64(&validPackets)
			currBatches := atomic.LoadUint64(&totalBatches)
			currFlushed := atomic.LoadUint64(&flushedItems)

			pps := currTotal - prevTotal
			prevTotal = currTotal

			fmt.Printf("📊 [METRICS] Speed: %d pkts/sec | Valid: %d | Queued -> Flushed: %d | Total Batches Flushed: %d\n",
				pps, currValid, currFlushed, currBatches)
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

		// ⚡ Push to Non-blocking In-Memory Channel Queue
		select {
		case dataQueue <- payload:
		default:
			// Buffer FULL Safety Catch (Drop logic if queue exceeds 1M items)
		}

		bufferPool.Put(bufPtr)
	}
}
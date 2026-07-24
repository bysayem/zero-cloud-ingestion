package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	SERVER_ADDR = "127.0.0.1:4242"
	NUM_NODES   = 1000 // 1,000 Concurrent Virtual ESP32 Nodes
	DURATION    = 10   // Test Duration in Seconds
)

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

func simulateNode(id uint32, stopChan chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	serverAddr, err := net.ResolveUDPAddr("udp", SERVER_ADDR)
	if err != nil {
		return
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		return
	}
	defer conn.Close()

	packet := make([]byte, 18)

	for {
		select {
		case <-stopChan:
			return
		default:
			// Pack 16-Byte Payload
			binary.LittleEndian.PutUint32(packet[0:4], id)
			binary.LittleEndian.PutUint64(packet[4:12], uint64(time.Now().UnixNano()))
			bits := math.Float32bits(25.0 + rand.Float32()*10.0) // Temp between 25-35°C
			binary.LittleEndian.PutUint32(packet[12:16], bits)

			// Calculate CRC16
			crc := calculateCRC16(packet[:16])
			binary.LittleEndian.PutUint16(packet[16:18], crc)

			// Send Packet
			conn.Write(packet)
		}
	}
}

func main() {
	fmt.Printf("🔥 Spawning %d Virtual ESP32 Nodes for High-Throughput Stress Test...\n", NUM_NODES)
	fmt.Printf("⏱️ Test will run for %d seconds...\n", DURATION)

	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Start 1,000 Nodes Parallelly
	for i := 1; i <= NUM_NODES; i++ {
		wg.Add(1)
		go simulateNode(uint32(i), stopChan, &wg)
	}

	// Run for DURATION seconds
	time.Sleep(time.Duration(DURATION) * time.Second)

	// Stop all nodes
	close(stopChan)
	wg.Wait()

	fmt.Println("\n🏁 Stress Test Completed!")
}
package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"time"
)

// Calculate CRC16 (Same algorithm as engine)
func calculateCRC16(data []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range data {
		curByte := uint16(b)
		for i := 0; i < 8; i++ {
			if (crc^curByte)&0x0001 != 0 {
				crc = (crc >> 1) ^ 0xA001 // Modbus CRC16 Polynomial
			} else {
				crc >>= 1
			}
			curByte >>= 1
		}
	}
	return crc
}

func main() {
	serverAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:4242")
	if err != nil {
		fmt.Println("Error resolving server address:", err)
		return
	}

	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		fmt.Println("Error connecting to server:", err)
		return
	}
	defer conn.Close()

	fmt.Println("🧪 Starting Ingestion Engine Security & Integrity Tests...")

	// ----------------------------------------------------
	// TEST 1: Sending a Valid 18-Byte Binary Packet
	// ----------------------------------------------------
	fmt.Println("\n[Test 1] Sending Valid Packet...")
	validBuf := make([]byte, 18)
	binary.LittleEndian.PutUint32(validBuf[0:4], 1001)                      // Device ID
	binary.LittleEndian.PutUint64(validBuf[4:12], uint64(time.Now().Unix())) // Timestamp
	bits := math.Float32bits(36.5)                                          // Temp
	binary.LittleEndian.PutUint32(validBuf[12:16], bits)

	// CRC16 Calculation for first 16 bytes
	crc := calculateCRC16(validBuf[:16])
	binary.LittleEndian.PutUint16(validBuf[16:18], crc)

	conn.Write(validBuf)
	time.Sleep(500 * time.Millisecond)

	// ----------------------------------------------------
	// TEST 2: Sending Invalid Size Packet (Corrupt Length)
	// ----------------------------------------------------
	fmt.Println("\n[Test 2] Sending Corrupt Length Packet (10 bytes)...")
	invalidSizeBuf := make([]byte, 10)
	conn.Write(invalidSizeBuf)
	time.Sleep(500 * time.Millisecond)

	// ----------------------------------------------------
	// TEST 3: Sending Corrupt CRC Packet (Tampered Data)
	// ----------------------------------------------------
	fmt.Println("\n[Test 3] Sending Tampered Data Packet (Bad CRC)...")
	corruptCrcBuf := make([]byte, 18)
	copy(corruptCrcBuf, validBuf)
	corruptCrcBuf[2] = 0xFF // Tamper DeviceID byte without updating CRC

	conn.Write(corruptCrcBuf)
	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n✅ Test execution completed!")
}
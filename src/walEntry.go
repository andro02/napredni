package src

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

type WalEntry struct {
	Crc       uint32
	Timestamp uint64
	Tombstone byte
	KeySize   uint64
	ValueSize uint64
	Key       []byte
	Value     []byte
}

func NewWalEntry() *WalEntry {

	walEntry := WalEntry{
		Crc:       0,
		Timestamp: uint64(time.Now().Unix()),
		Tombstone: 0,
		KeySize:   0,
		ValueSize: 0,
		Key:       nil,
		Value:     nil,
	}
	return &walEntry

}

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (walEntry *WalEntry) Write(key string, value []byte) {

	walEntry.Key = []byte(key)
	walEntry.Value = value
	walEntry.KeySize = uint64(len(walEntry.Key))
	walEntry.ValueSize = uint64(len(walEntry.Value))
	walEntry.Crc = CRC32(walEntry.ToBytes())

}

func (walEntry *WalEntry) ToBytes() []byte {

	bytes := make([]byte, 0)

	crc := make([]byte, 4)
	binary.LittleEndian.PutUint32(crc, walEntry.Crc)
	bytes = append(bytes, crc...)

	timestamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp, walEntry.Timestamp)
	bytes = append(bytes, timestamp...)

	bytes = append(bytes, walEntry.Tombstone)

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, walEntry.KeySize)
	bytes = append(bytes, keySize...)

	valueSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSize, walEntry.ValueSize)
	bytes = append(bytes, valueSize...)

	bytes = append(bytes, walEntry.Key...)
	bytes = append(bytes, walEntry.Value...)

	return bytes

}

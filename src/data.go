package src

type ElementData interface {
	Key() []byte
	Value() []byte
	Tombstone() byte
	Timestamp() int64
	SetKey([]byte)
	SetValue([]byte)
	SetTimestamp(int64)
	SetTombstone(byte)
}

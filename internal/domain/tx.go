package domain

type Transaction struct {
	ID        string
	Index     int
	Timestamp int64
	Encoding  int
	Data      []byte
	Action    string
}

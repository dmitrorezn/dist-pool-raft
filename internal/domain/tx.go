package domain

type Transaction struct {
	ID        string `json:"id"`
	Index     int    `json:"index"`
	Timestamp int64  `json:"timestamp"`
	Encoding  int    `json:"encoding"`
	Data      string `json:"data"`
	Action    string `json:"action"`
}

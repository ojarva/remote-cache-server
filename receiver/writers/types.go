package writers

// Writer defines interface for outputting data to various destinations.
type Writer interface {
	Write([]byte) (int, error)
	Init() error
	Close() error
}

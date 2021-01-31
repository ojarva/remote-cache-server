package writers

import "fmt"

// StdoutWriter writes all incoming data to stdout
type StdoutWriter struct {
}

func (sw *StdoutWriter) Write(output []byte) (int, error) {
	fmt.Println(string(output))
	return len(output), nil
}

// Init initializes the writer. With StdoutWriter this is a no-op.
func (sw *StdoutWriter) Init() error {
	return nil
}

// Close closes the writer. With StdoutWriter this is a no-op.
func (sw *StdoutWriter) Close() error {
	return nil
}

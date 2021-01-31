package writers

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

var outputFilenameFlag = flag.String("output-filename", "", "Output filename")

// FileWriter writes output to a file. Recycling file is not supported (i.e., if file is renamed, FileWriter continues writing to a new file).
type FileWriter struct {
	fileHandle  *os.File
	initialized bool
}

func (fw *FileWriter) Write(output []byte) (int, error) {
	c, err := fw.fileHandle.Write(output)
	if err != nil {
		return c, err
	}
	c2, err := fw.fileHandle.WriteString("\n")
	return c + c2, err
}

// Close finalizes writes and handles closing file handles.
func (fw *FileWriter) Close() error {
	if fw.initialized {
		return fw.fileHandle.Close()
	}
	return errors.New("Calling close on uninitialized writer")
}

// Init initializes FileWriter, opening the file handle.
func (fw *FileWriter) Init() error {
	if *outputFilenameFlag == "" {
		return errors.New("Filename must be specified for writing to file")
	}
	fh, err := os.OpenFile(*outputFilenameFlag, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("Opening output file %s failed with %s", *outputFilenameFlag, err)
	}
	fw.fileHandle = fh
	fw.initialized = true
	return nil
}

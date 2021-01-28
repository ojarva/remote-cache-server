package backends

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ojarva/remote-cache-server/types"
)

// FileCacheBackend is a basic backend for storing overflow data in the filesystem, inside a single folder.
type FileCacheBackend struct {
	CacheDir        string
	lastIdentifier  uint64
	identifierMutex sync.Mutex
}

// Init initializes FileCacheBackend, checking preconditions (such as existence of the cache directory) and loads current identifier, if available.
func (fcb *FileCacheBackend) Init() error {
	info, err := os.Stat(fcb.CacheDir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", fcb.CacheDir)
	}
	fullIdentifier, err := fcb.GetNewestIdentifier()
	if err == nil {
		identifierString := strings.Split(fullIdentifier, "-")[0]
		parsedIdentifier, err := strconv.ParseUint(identifierString, 10, 64)
		if err != nil {
			return fmt.Errorf("Invalid identifier from filename: %s", err)
		}
		(*fcb).lastIdentifier = parsedIdentifier
	}
	return nil
}

func (fcb *FileCacheBackend) getFilename(identifier string) string {
	return filepath.Join(fcb.CacheDir, identifier)
}

func (fcb *FileCacheBackend) getCacheFilename(ob types.OutgoingBatch) string {
	identifier := atomic.AddUint64(&fcb.lastIdentifier, 1)
	return fmt.Sprintf("%020d-%s-%s.upload_cache", identifier, time.Now().Format(time.RFC3339Nano), ob.BatchID)
}

func (fcb *FileCacheBackend) getAllFiles() []string {
	files, err := filepath.Glob(filepath.Join(fcb.CacheDir, "*.upload_cache"))
	if err != nil {
		panic(fmt.Sprintf("Unable to list files: %s", err))
	}
	sort.Strings(files)
	return files
}

// GetOldestIdentifier returns the oldest identifier. An error is returned if no files are available.
func (fcb *FileCacheBackend) GetOldestIdentifier() (string, error) {
	files := fcb.getAllFiles()
	if len(files) > 0 {
		return filepath.Base(files[0]), nil
	}
	return "", fmt.Errorf("Unable to get the oldest identifier: no files available")
}

// GetNewestIdentifier returns the newest identifier. An error is returned if no files are available.
func (fcb *FileCacheBackend) GetNewestIdentifier() (string, error) {
	files := fcb.getAllFiles()
	if len(files) > 0 {
		return filepath.Base(files[len(files)-1]), nil
	}
	return "", fmt.Errorf("Unable to get the newest identifier: no files available")
}

// GetCachedContent gets cached file contents. File is not removed from the disk.
func (fcb *FileCacheBackend) GetCachedContent(identifier string) ([]string, error) {
	filename := fcb.getFilename((identifier))
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Unable to read cached file: %s - %s", identifier, err)
		return nil, err
	}
	defer file.Close()
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// DeleteCacheItem removes item from cache
func (fcb *FileCacheBackend) DeleteCacheItem(identifier string) error {
	filename := fcb.getFilename((identifier))
	log.Printf("Deleting %s", filename)
	return os.Remove(filename)
}

// Save saves OutgoingBatch to persistent storage
func (fcb *FileCacheBackend) Save(ob types.OutgoingBatch) error {
	filename := fcb.getFilename(fcb.getCacheFilename(ob))
	log.Printf("Saving to %s", filename)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("Unable to save to %s - %s", filename, err)
	}
	defer file.Close()
	for _, item := range ob.Values {
		file.WriteString(item + "\n")
	}
	return nil
}

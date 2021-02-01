package backends

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/ojarva/remote-cache-server/sender/types"
)

func TestFileBackendFilenames(t *testing.T) {
	fcb := FileCacheBackend{}
	fcb.getCacheFilename(types.OutgoingBatch{BatchID: "teststring"})
	fcb.getCacheFilename(types.OutgoingBatch{BatchID: "teststring"})
	filename := fcb.getCacheFilename(types.OutgoingBatch{BatchID: "teststring"})
	if !strings.HasPrefix(filename, "00000000000000000003") {
		t.Errorf("Invalid filename: %s", filename)

	}
}

func TestFileBackendNoDirectory(t *testing.T) {
	fcb := FileCacheBackend{CacheDir: "non-existing-directory"}
	err := fcb.Init()
	if err == nil {
		t.Error("Init did not return an error")
	}
}

func TestFileBackendInvalidIdentifier(t *testing.T) {
	randomDirectoryID := uuid.New().String()
	os.Mkdir(randomDirectoryID, 0700)
	defer os.RemoveAll(randomDirectoryID)
	os.Create(filepath.Join(randomDirectoryID, "invalidfilename.upload_cache"))
	fcb := FileCacheBackend{CacheDir: randomDirectoryID}
	err := fcb.Init()
	if err == nil {
		t.Error("Init did not return an error")
	}
}

func TestEmptyFileBackend(t *testing.T) {
	randomDirectoryID := uuid.New().String()
	os.Mkdir(randomDirectoryID, 0700)
	defer os.RemoveAll(randomDirectoryID)
	fcb := FileCacheBackend{CacheDir: randomDirectoryID}
	err := fcb.Init()
	if err != nil {
		t.Errorf("Init failed: %s", err)
	}
	filename := fcb.getCacheFilename(types.OutgoingBatch{BatchID: "testbatch"})
	if !strings.HasPrefix(filename, "00000000000000000001") {
		t.Errorf("Invalid filename: %s", filename)
	}
}

func TestNonEmptyFileBackend(t *testing.T) {
	randomDirectoryID := uuid.New().String()
	os.Mkdir(randomDirectoryID, 0700)
	defer os.RemoveAll(randomDirectoryID)
	os.Create(filepath.Join(randomDirectoryID, "00000000000000000530-2021-01-27T15:19:40.635457+02:00-9ec79b0a-c737-4515-af59-eccd98fc853e.upload_cache"))
	os.Create(filepath.Join(randomDirectoryID, "00000000000000000531-2021-01-27T15:19:40.635457+02:00-9ec79b0a-c737-4515-af59-eccd98fc853e.upload_cache"))
	fcb := FileCacheBackend{CacheDir: randomDirectoryID}
	err := fcb.Init()
	if err != nil {
		t.Errorf("Init failed: %s", err)
	}
	filename := fcb.getCacheFilename(types.OutgoingBatch{BatchID: "testbatch"})
	if !strings.HasPrefix(filename, "00000000000000000532") {
		t.Errorf("Invalid filename: %s", filename)
	}
}

func TestFileBackendOps(t *testing.T) {
	randomDirectoryID := uuid.New().String()
	os.Mkdir(randomDirectoryID, 0700)
	defer os.RemoveAll(randomDirectoryID)
	fcb := FileCacheBackend{CacheDir: randomDirectoryID}
	var err error
	err = fcb.Init()
	if err != nil {
		t.Errorf("Init failed: %s", err)
	}
	fcb.Save(types.OutgoingBatch{BatchID: "testid3214", Values: []string{"test1", "test2"}})
	fcb.Save(types.OutgoingBatch{BatchID: "testid1234", Values: []string{"test3", "test4"}})
	_, err = fcb.GetOldestIdentifier()
	if err != nil {
		t.Errorf("GetOldestIdentifier did not return an error")
	}
	newestFile, err := fcb.GetNewestIdentifier()
	if err != nil {
		t.Errorf("Failed with %s", err)
	}
	oldestFile, err := fcb.GetOldestIdentifier()
	if err != nil {
		t.Errorf("Failed with %s", err)
	}
	if !strings.HasPrefix(newestFile, "00000000000000000002") {
		t.Errorf("Invalid newest filename: %s", newestFile)
	}
	if !strings.HasPrefix(oldestFile, "00000000000000000001") {
		t.Errorf("Invalid oldest filename: %s", err)
	}
	content, err := fcb.GetCachedContent(oldestFile)
	if err != nil {
		t.Errorf("Failed with %s", err)
	}
	if len(content) != 2 || content[0] != "test1" || content[1] != "test2" {
		t.Errorf("Invalid content: %s", content)
	}
	fcb.DeleteCacheItem(oldestFile)
	content, err = fcb.GetCachedContent(oldestFile)
	if err == nil {
		t.Error("GetCachedContent did not fail")
	}
}

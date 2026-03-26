package dataset

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

type Record[T interface{}] struct {
	EntityID  string `json:"entityId"` // The ID used for LWW compaction
	EventID   string `json:"eventId"`  // Unique ID for this specific log entry
	Timestamp int64  `json:"ts"`       // UnixNano for precise LWW ordering
	ChangedBy string `json:"changedBy"`
	Deleted   bool   `json:"deleted"`
	Data      T      `json:"data"`
}

type Dataset[T any] struct {
	location string
	mu       sync.Mutex // Ensures thread-safety for concurrent writes
}

// NewDataset initializes the file once
func NewDataset[T any](path string) *Dataset[T] {
	return &Dataset[T]{location: path}
}

func (ds *Dataset[T]) Write(entityID string, data T, changedBy string, isDeleted bool) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	r := Record[T]{
		EntityID:  entityID,
		EventID:   uuid.NewString(),
		Timestamp: time.Now().UnixNano(),
		Deleted:   isDeleted,
		ChangedBy: changedBy,
		Data:      data,
	}

	payload, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	f, err := os.OpenFile(ds.location, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write error: %w", err)
	}

	return nil
}

// ReadCurrentState processes the JSONL file and returns only the latest version of each entity.
func (ds *Dataset[T]) ReadCurrentState() ([]T, error) {

	latestState, err := readCurrentState(ds.location)
	if err != nil {
		return nil, err
	}

	results := make([]T, 0, len(latestState))

	for entityID, record := range latestState {
		var item T
		if err := json.Unmarshal(record.Data, &item); err != nil {
			log.Printf("Skipping malformed entity [%s]: %v", entityID, err)
			continue
		}
		results = append(results, item)
	}

	return results, nil
}

func (ds *Dataset[T]) ArchiveAndChecksum() error {

	if _, err := os.Stat(ds.location); os.IsNotExist(err) {
		return nil
	}

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	processingPath := ds.location + ".processing"
	archivePath := fmt.Sprintf("%s.%s.jsonl.gz", ds.location, timestamp)
	checksumPath := archivePath + ".sha256"

	if err := ds.Checkpoint(processingPath); err != nil {
		return err
	}

	// 3. Compress the file
	if err := compressFile(processingPath, archivePath); err != nil {
		return fmt.Errorf("compression failed: %w", err)
	}

	hash, _ := generateSHA256(archivePath)
	os.WriteFile(checksumPath, []byte(hash), 0644)

	return os.Remove(processingPath)
}

// CompactToParquet moves the current JSONL to a temp file,
// processes LWW, and saves a Parquet snapshot.
func (ds *Dataset[T]) CompactToParquet() error {

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	processingPath := ds.location + ".processing"
	parquetPath := fmt.Sprintf("%s.%s.parquet", ds.location, timestamp)
	checksumPath := parquetPath + ".sha256"

	// 1. Rotate the file
	// If the file doesn't exist (no writes yet), just exit early.
	if _, err := os.Stat(ds.location); os.IsNotExist(err) {
		return nil
	}

	if err := os.Rename(ds.location, processingPath); err != nil {
		return fmt.Errorf("failed to rotate file: %w", err)
	}

	// 2. Read the "State" from the rotated file
	// We temporarily point a new dataset instance at the .compacting file
	tempDs := &Dataset[T]{location: processingPath}
	records, err := tempDs.ReadCurrentState()
	if err != nil {
		return fmt.Errorf("failed to read state for compaction: %w", err)
	}

	// 3. Write to Parquet
	if len(records) > 0 {
		pf, err := os.Create(parquetPath)
		if err != nil {
			return fmt.Errorf("failed to create parquet file: %w", err)
		}
		defer pf.Close()

		if err = parquet.Write(pf, records); err != nil {
			return fmt.Errorf("parquet write error: %w", err)
		}

		hash, _ := generateSHA256(parquetPath)
		os.WriteFile(checksumPath, []byte(hash), 0644)
	}

	return os.Remove(processingPath)
}

func (ds *Dataset[T]) Checkpoint(archivePath string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if _, err := os.Stat(ds.location); os.IsNotExist(err) {
		return nil
	}

	state, err := readCurrentState(ds.location)
	if err != nil {
		return fmt.Errorf("failed to read state for checkpoint: %w", err)
	}

	backupPath := ds.location + ".bak"

	if archivePath != "" {
		backupPath = archivePath
	}

	if err := os.Rename(ds.location, backupPath); err != nil {
		return fmt.Errorf("failed to rename for checkpoint: %w", err)
	}

	newFile, err := os.OpenFile(ds.location, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		// Critical: if we can't create the new file, we should try to move the backup back
		os.Rename(backupPath, ds.location)
		return fmt.Errorf("failed to create checkpoint: %w", err)
	}
	defer newFile.Close()

	for _, record := range state {
		// Skip records that are marked as deleted - we don't need them in a fresh start
		if record.Deleted {
			continue
		}

		payload, err := json.Marshal(record)
		if err != nil {
			continue // Skip malformed records
		}

		if _, err := newFile.Write(append(payload, '\n')); err != nil {
			return fmt.Errorf("failed writing to checkpoint: %w", err)
		}
	}

	if archivePath != "" {
		return nil
	}

	return os.Remove(backupPath)
}

// Internal helper for Gzip
func compressFile(src, dst string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	gw := gzip.NewWriter(out)
	defer gw.Close()

	_, err = io.Copy(gw, f)
	return err
}

// Internal helper for SHA256
func generateSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func readCurrentState(src string) (map[string]Record[json.RawMessage], error) {
	// We open a fresh read-only handle to avoid interfering with the writer
	f, err := os.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	latestState := make(map[string]Record[json.RawMessage])
	scanner := bufio.NewScanner(f)

	// const maxLineSize = 1024 * 1024 // 1MB
	// scanner.Buffer(make([]byte, 64*1024), maxLineSize)

	line := 0

	for scanner.Scan() {

		line++

		var r Record[json.RawMessage]
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			log.Printf("Skipping malformed line (%d): %v", line, err)
			continue
		}

		// LWW Logic:
		// 1. If we haven't seen this ID, add it.
		// 2. If we HAVE seen it, only update if the new timestamp is higher.
		existing, found := latestState[r.EntityID]
		if !found || r.Timestamp > existing.Timestamp {
			latestState[r.EntityID] = r
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Optional: Filter out records marked as 'Deleted' before returning
	fullState := filterDeletes[json.RawMessage](latestState)

	return fullState, nil
}

func filterDeletes[T any](m map[string]Record[T]) map[string]Record[T] {
	for id, r := range m {
		if r.Deleted {
			delete(m, id)
		}
	}
	return m
}

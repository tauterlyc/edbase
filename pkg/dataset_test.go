package edbase

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type User struct {
	Name  string `json:"name"`
	Age int `json:"age"`
}

func TestDataset_Lifecycle(t *testing.T) {
	// 1. Setup: Create a temporary directory and initialize dataset
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_data.jsonl")

	ds := NewDataset[User](path)

	// 2. Test Writing: Add initial state and an update
	entityID := "user_123"

	// First write
	err := ds.Write(entityID, User{Name: "Alice", Age: 30}, "admin_1", false)
	assert.NoError(t, err, "First write failed")

	// Second write (The "Update" for LWW)
	err = ds.Write(entityID, User{Name: "Alice Smith", Age: 47}, "user_123", false)
	assert.NoError(t, err, "Update write failed")

	// 3. Test Reading: Verify Last-Write-Wins logic
	state, err := ds.ReadCurrentState()
	require.NoError(t, err, "ReadCurrentState failed")

	assert.Len(t, state, 1, "Expected 1 unique record")

	assert.Equal(t, 47, state[0].Age, "LWW failed")
}

func TestDataset_Tombstone(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_delete.jsonl")

	ds := NewDataset[User](path)

	entityID := "user_999"

	// Write then Delete
	ds.Write(entityID, User{Name: "To Be Deleted"}, "admin", false)
	ds.Write(entityID, User{}, "admin", true) // The Tombstone

	state, err := ds.ReadCurrentState()
	assert.NoError(t, err)

	assert.Len(t, state, 0, "Expected 0 records after tombstone")
}

func TestDataset_MultipleEntities(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_multi.jsonl")

	ds := NewDataset[User](path)

	// Write two different users
	ds.Write("u1", User{Name: "User 1"}, "sys", false)
	ds.Write("u2", User{Name: "User 2"}, "sys", false)

	state, _ := ds.ReadCurrentState()
	assert.Len(t, state, 2, "Expected 2 records")
}

type TestData struct {
	Value string `json:"value"`
}

func TestDataset_Checkpoint_Compaction(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "data.jsonl")
	ds := NewDataset[TestData](path)

	// 1. Write multiple versions of the same entity
	entityID := "item_1"
	ds.Write(entityID, TestData{Value: "v1"}, "user1", false)
	ds.Write(entityID, TestData{Value: "v2"}, "user1", false)
	ds.Write(entityID, TestData{Value: "v3"}, "user1", false)

	// 2. Run Checkpoint without archiving
	err := ds.Checkpoint("")
	require.NoError(t, err, "Checkpoint failed")

	// 3. Verify file only has ONE line now (the latest version)
	f, _ := os.Open(path)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	lineCount := 0
	var lastRecord Record[TestData]
	for scanner.Scan() {
		lineCount++
		json.Unmarshal(scanner.Bytes(), &lastRecord)
	}

	assert.Equal(t, 1, lineCount, "Expected 1 line after checkpoint")
	assert.Equal(t, "v3", lastRecord.Data.Value)
}

func TestDataset_Checkpoint_Archive(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "data.jsonl")
	archivePath := filepath.Join(tmpDir, "history_backup.jsonl")
	ds := NewDataset[TestData](path)

	// Write some data
	ds.Write("item_1", TestData{Value: "important"}, "user1", false)

	// Run Checkpoint with an archive path
	err := ds.Checkpoint(archivePath)
	if err != nil {
		t.Fatalf("Checkpoint with archive failed: %v", err)
	}

	// Verify the archive file exists
	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		t.Error("Archive file was not created")
	}

	// Verify the main file still exists (re-populated)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Main file missing after checkpoint")
	}
}

func TestDatasetCheckpointDeletes(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "data.jsonl")
	ds := NewDataset[TestData](path)

	// Write a record then delete it
	ds.Write("ghost", TestData{Value: "here"}, "user1", false)
	ds.Write("ghost", TestData{}, "user1", true) // Deleted

	// Run Checkpoint
	ds.Checkpoint("")

	// The file should now be empty (0 bytes) because the only record was deleted
	info, _ := os.Stat(path)
	if info.Size() != 0 {
		t.Errorf("Expected empty file after checkpointing a deleted record, got size %d", info.Size())
	}
}


// TestBasicLWW ensures that multiple writes to the same ID only return the latest version
func TestDataset_LWW(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "test.jsonl")
	ds := NewDataset[User](path)

	id := "user-1"
	ds.Write(id, User{"Alice", 25}, "admin", false)
	ds.Write(id, User{"Alice Smith", 26}, "admin", false)

	state, _ := ds.ReadCurrentState()
	if len(state) != 1 {
		t.Fatalf("expected 1 record, got %d", len(state))
	}
	if state[0].Name != "Alice Smith" {
		t.Errorf("LWW failed, got name: %s", state[0].Name)
	}
}

// TestCheckpointDeletes ensures that Checkpoint physically removes tombstoned records
func TestDataset_Checkpoint_Deletes(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "test.jsonl")
	ds := NewDataset[User](path)

	ds.Write("user-1", User{"Bob", 30}, "admin", false)
	ds.Write("user-1", User{}, "admin", true) // Delete

	if err := ds.Checkpoint(""); err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}

	// Read raw file size to ensure it's empty
	info, _ := os.Stat(path)
	if info.Size() != 0 {
		t.Errorf("file should be empty after checkpointing a delete, got size %d", info.Size())
	}
}

// TestArchiveAndChecksum verifies that archiving creates the .gz and the .sha256 files
func TestDataset_ArchiveAndChecksum(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "test.jsonl")
	ds := NewDataset[User](path)

	ds.Write("user-1", User{"Charlie", 40}, "admin", false)

	if err := ds.ArchiveAndChecksum(); err != nil {
		t.Fatalf("archive failed: %v", err)
	}

	// ArchiveAndChecksum renames the live file to .processing via Checkpoint, 
	// then compresses it.
	files, _ := os.ReadDir(tmp)
	foundGz := false
	foundSha := false
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".gz" { foundGz = true }
		if filepath.Ext(f.Name()) == ".sha256" { foundSha = true }
	}

	if !foundGz || !foundSha {
		t.Errorf("missing backup files. Gz: %v, Sha: %v", foundGz, foundSha)
	}
}
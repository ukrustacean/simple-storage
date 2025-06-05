package datastore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp, 1*Mb)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp, 1*Mb)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})

	t.Run("put/get int64", func(t *testing.T) {
		key := "int_key"
		var value int64 = 10

		err := db.PutInt64(key, value)
		if err != nil {
			t.Fatalf("PutInt64 failed: %v", err)
		}

		got, err := db.GetInt64(key)
		if err != nil {
			t.Fatalf("GetInt64 failed: %v", err)
		}

		if got != value {
			t.Errorf("GetInt64(%q) = %d, want %d", key, got, value)
		}
	})

	t.Run("int64 wrong type", func(t *testing.T) {
		strKey := "string_key"
		err := db.Put(strKey, "not int")
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		_, err = db.GetInt64(strKey)
		if err == nil {
			t.Fatalf("expected type error when reading string as int64")
		}
	})

	t.Run("string wrong type", func(t *testing.T) {
		intKey := "int_key_2"
		var intVal int64 = 10

		err := db.PutInt64(intKey, intVal)
		if err != nil {
			t.Fatalf("PutInt64 failed: %v", err)
		}

		_, err = db.Get(intKey)
		if err == nil {
			t.Fatalf("expected type error when reading int64 as string")
		}
	})
}

func TestMergeSegments(t *testing.T) {
	tmp := t.TempDir()

	db, err := Open(tmp, 50)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	entries := map[string]string{
		"one":   "11111111111111111111",
		"two":   "22222222222222222222",
		"three": "33333333333333333333",
	}

	for k, v := range entries {
		if err := db.Put(k, v); err != nil {
			t.Fatalf("failed to put key %q: %v", k, err)
		}
	}

	segCountBefore := len(db.segments)
	if segCountBefore <= 1 {
		t.Fatalf("expected multiple segments before merge, got %d", segCountBefore)
	}

	err = db.MergeSegments()
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}

	if len(db.segments) != 1 {
		t.Fatalf("expected 1 segment after merge, got %d", len(db.segments))
	}

	for k, v := range entries {
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("failed to get key %q after merge: %v", k, err)
		}
		if got != v {
			t.Errorf("key %q: expected %q, got %q", k, v, got)
		}
	}

	files, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("failed to read temp tmp: %v", err)
	}
	for _, f := range files {
		if !filepath.HasPrefix(f.Name(), outFileName+"-") {
			continue
		}
		t.Logf("Remaining file: %s", f.Name())
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file after merge, found %d", len(files))
	}
}

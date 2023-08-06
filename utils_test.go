package s3client

import (
	"bytes"
	"compress/gzip"
	"testing"
)

func TestIsGlobPattern(t *testing.T) {
	tests := []struct {
		s      string
		isGlob bool
	}{
		{"dir/*.txt", true},
		{"dir1/dir2/*.txt", true},
		{"dir/subdir/*/file.txt", true},
		{"*.txt", true},
		{"file.txt", false},
		{"dir1/dir2/dir3/*/*/*.txt", true},
		{"dir/[a-z]*.txt", true},
		{"dir/subdir/file?.txt", true},
		{"dir/subdir/file\\.txt", true},
		{"dir1/dir2/dir3", false},
	}
	for _, test := range tests {
		isGlob := IsGlobPattern(test.s)
		if isGlob != test.isGlob {
			t.Errorf("Expected IsGlobPattern(%q) to return %v, but got %v", test.s, test.isGlob, isGlob)
		}
	}
}

func TestGetDirPrefix(t *testing.T) {
	tests := []struct {
		glob      string
		dirPrefix string
	}{
		{"dir/*.txt", "dir"},
		{"dir1/dir2/*.txt", "dir1/dir2"},
		{"dir/subdir/*/file.txt", "dir/subdir"},
		{"*.txt", ""},
		{"file.txt", "file.txt"},
		{"dir1/dir2/dir3/*/*/*.txt", "dir1/dir2/dir3"},
		{"dir/[a-z]*.txt", "dir"},
		{"/file/test.txt", "/file/test.txt"},
	}
	for _, test := range tests {
		dirPrefix := GetDirPrefix(test.glob)
		if dirPrefix != test.dirPrefix {
			t.Errorf("Expected GetDirPrefix(%q) to return %q, but got %q", test.glob, test.dirPrefix, dirPrefix)
		}
	}
}

func TestIsBinaryContentType(t *testing.T) {
	testCases := []struct {
		contentType string
		expected    bool
	}{
		{"application/octet-stream", true},
		{"application/gzip", true},
		{"application/x-tar", true},
		{"application/pdf", false},
		{"text/plain", false},
		{"image/jpeg", false},
		{"", false},
		{"invalid", false},
	}

	for _, tc := range testCases {
		result := IsBinaryContentType(tc.contentType)
		if result != tc.expected {
			t.Errorf("IsBinaryContentType(%q) returned %v, expected %v", tc.contentType, result, tc.expected)
		}
	}
}

func TestGetFileReader(t *testing.T) {
	// Test the gzip reader
	gzReaderFunc := GetFileReader("test.gz")

	// Compress the data using gzip
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, err := w.Write([]byte("test data"))
	if err != nil {
		return
	}
	err = w.Close()
	if err != nil {
		return
	}

	gzReader, err := gzReaderFunc(bytes.NewReader(b.Bytes()))
	if err != nil {
		t.Errorf("Unexpected error when reading gzip file: %v", err)
	}
	if gzReader == nil {
		t.Error("Expected gzip reader, got nil")
	}

	// Test the tar reader
	tarReaderFunc := GetFileReader("test.tar")
	tarReader, err := tarReaderFunc(bytes.NewReader([]byte("test data")))
	if err != nil {
		t.Errorf("Unexpected error when reading tar file: %v", err)
	}
	if tarReader == nil {
		t.Error("Expected tar reader, got nil")
	}

	// Test the default reader
	defaultReaderFunc := GetFileReader("test.txt")
	defaultReader, err := defaultReaderFunc(bytes.NewReader([]byte("test data")))
	if err != nil {
		t.Errorf("Unexpected error when reading default file: %v", err)
	}
	if defaultReader == nil {
		t.Error("Expected default reader, got nil")
	}
}

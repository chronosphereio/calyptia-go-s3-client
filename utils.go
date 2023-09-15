package s3client

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"mime"
	"path/filepath"
	"strings"
)

type gzipReader struct {
	*gzip.Reader
}

func (gr *gzipReader) Read(p []byte) (n int, err error) {
	return gr.Reader.Read(p)
}

type tarReader struct {
	*tar.Reader
}

func (tr *tarReader) Read(p []byte) (n int, err error) {
	return tr.Reader.Read(p)
}

// IsBinaryContentType returns true if the given content type is a binary content type,
// and false otherwise.
func IsBinaryContentType(contentType string) bool {
	// Parse the content type to get the media type and parameters
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		// If the content type is invalid, assume it is not binary
		return false
	}

	// Check if the media type is in the list of known binary media types
	switch mediaType {
	case "application/octet-stream", "application/gzip", "application/x-tar", "application/tar+gzip":
		return true
	default:
		return false
	}
}

// GetFileReader returns a function that creates a reader for a given file,
// based on the file's extension.
// The returned function takes an io.Reader as input and returns an io.Reader
// and an error, if any.
func GetFileReader(filename string) func(io.Reader) (io.ReadCloser, error) {
	// Get the file extension of the given file
	extension := strings.ToLower(filepath.Ext(filename))

	// Return the appropriate reader function depending on the file extension
	switch {
	case extension == ".gz" || extension == ".gzip":
		return func(r io.Reader) (io.ReadCloser, error) {
			// read the entire body from the reader.
			// this should be buffered and with a seeker
			body, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}

			orig := body
			gr, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				// See https://github.com/aws/aws-sdk-go/issues/1292
				// The default HTTP transports that the AWS SDK uses will decompress objects transparently
				// if the Content Encoding is gzip. Not everyone or everything properly sets the Content-Encoding
				// header on their S3 objects, so we could be trying to process gzipped objects and not know it.
				if err == gzip.ErrHeader {
					rc := io.NopCloser(bytes.NewReader(orig))
					return rc, nil
				}
				return nil, err
			}
			return gr, nil
		}
	case extension == ".tar":
		return func(r io.Reader) (io.ReadCloser, error) {
			tr := io.NopCloser(tar.NewReader(r))
			return tr, nil
		}
	default:
		return func(r io.Reader) (io.ReadCloser, error) {
			rc := io.NopCloser(r)
			return rc, nil
		}
	}
}

// IsGlobPattern returns true if the given string is a glob pattern.
func IsGlobPattern(s string) bool {
	// Check if the string contains any of the special glob characters: *, ?, [, or \
	return strings.ContainsAny(s, "*?[\\")
}

// GetDirPrefix returns the directory prefix from a glob expression.
func GetDirPrefix(glob string) string {
	// Split the glob expression by the path separator
	parts := strings.Split(glob, string(filepath.Separator))

	// Find the index of the last wildcard
	lastWildcardIndex := -1
	for i := len(parts) - 1; i >= 0; i-- {
		if strings.ContainsAny(parts[i], "*?[") {
			lastWildcardIndex = i
		}
	}

	// If there is a wildcard, return the part before it
	if lastWildcardIndex >= 0 {
		return strings.Join(parts[:lastWildcardIndex], string(filepath.Separator))
	}

	// If there are no wildcards, the whole expression is the directory prefix
	return glob
}

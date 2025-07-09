import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"mime"
	"path/filepath"
	"strings"
)

func GetFileReader(filename string) func(io.Reader) (io.ReadCloser, error) {
	// Get the file extension of the given file
	extension := strings.ToLower(filepath.Ext(filename))

	// Return the appropriate reader function depending on the file extension
	switch {
	case extension == ".gz" || extension == ".gzip":
		return func(r io.Reader) (io.ReadCloser, error) {
			// We need to buffer the data because gzip.NewReader consumes bytes
			// to check the header, and if it fails, we need the original data
			body, err := io.ReadAll(r)
			if err != nil {
				return nil, err
			}

			// Try to create a gzip reader from the buffered data
			gr, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				// See https://github.com/aws/aws-sdk-go/issues/1292
				// The default HTTP transports that the AWS SDK uses will decompress objects transparently
				// if the Content Encoding is gzip. Not everyone or everything properly sets the Content-Encoding
				// header on their S3 objects, so we could be trying to process gzipped objects and not know it.
				if errors.Is(err, gzip.ErrHeader) {
					// If it's not actually gzipped, return the original buffered data
					rc := io.NopCloser(bytes.NewReader(body))
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

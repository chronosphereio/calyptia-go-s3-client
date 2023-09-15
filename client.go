package s3client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bmatcuk/doublestar"
	"github.com/calyptia/go-s3-client/ifaces"
	"github.com/calyptia/plugin"
	"io"
	"net/http"
	"path/filepath"
)

type (
	// Client is the interface for interacting with an S3 bucket.
	Client interface {
		ListFiles(ctx context.Context, bucket, pattern string) ([]string, error)
		ReadFile(ctx context.Context, bucket string, file string, initialBufferSize int, maxBufferSize int) (<-chan string, <-chan error)
	}
	// DefaultClient is a concrete implementation of the Client interface that uses the AWS SDK for Go to interact with S3.
	DefaultClient struct {
		Client
		Svc    ifaces.Client
		Logger plugin.Logger
	}
)

// New returns a new DefaultClient configured with the given options and using the provided logger.
func New(ctx context.Context, logger plugin.Logger, optsFns ...ClientOptsFunc) (*DefaultClient, error) {
	var opts ClientOpts
	for _, optFn := range optsFns {
		err := optFn(&opts)
		if err != nil {
			return nil, err
		}
	}

	cfg, err := config.LoadDefaultConfig(ctx, opts.LoadOptions()...)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(options *s3.Options) {
		//	https://github.com/minio/minio/discussions/12030#discussioncomment-590564
		//	this is backwards compatible flag to make it work with minio.
		options.UsePathStyle = true
		options.HTTPClient = &http.Client{
			Transport: &http.Transport{
				DisableCompression: true,
			},
		}
		if opts.Endpoint != "" {
			options.BaseEndpoint = &opts.Endpoint
		}
	})

	return &DefaultClient{Svc: client, Logger: logger}, nil
}

// ListFiles returns a list of file names in the specified bucket that match the given pattern.
func (c *DefaultClient) ListFiles(ctx context.Context, bucket, pattern string) ([]string, error) {
	var files []string
	// listAndMatch is a helper function that lists objects in the bucket with the given prefix and file name,
	// and applies the given match function to each object name. If the match function returns true,
	// the object name is added to the files slice.
	listAndMatch := func(bucket, pattern string, match func(objectName string) bool) ([]string, error) {
		// List objects in the S3 bucket with the given prefix and file name
		params := &s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
		}

		prefix := GetDirPrefix(pattern)
		if prefix != "" {
			params.Prefix = &prefix
		}

		c.Logger.Debug("listing files on bucket: %q with prefix: %q that follows pattern: %q", bucket, prefix, pattern)
		p := s3.NewListObjectsV2Paginator(c.Svc, params)

		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return files, err
			}
			for _, obj := range page.Contents {
				matches := match(*obj.Key)
				c.Logger.Debug("object key: %q matches with pattern: %q result: %q", *obj.Key, pattern, matches)
				if matches {
					files = append(files, *obj.Key)
				}
			}
		}
		c.Logger.Debug("found: %d file(s) on bucket: %q that follows pattern: %q", len(files), bucket, pattern)
		return files, nil
	}

	files, err := listAndMatch(bucket, pattern, func(objectName string) bool {
		if IsGlobPattern(pattern) {
			matches, err := doublestar.PathMatch(pattern, objectName)
			return err == nil && matches
		}
		return filepath.Base(pattern) == filepath.Base(objectName)
	})
	if err != nil {
		return files, fmt.Errorf("error listing files from s3: %w", err)
	}

	return files, nil
}

// ReadFile reads the specified file from the given S3 bucket and sends its contents
// line by line through a channel. It uses an adaptive buffering mechanism to handle
// large lines of text up to a specified maximum size.
func (c *DefaultClient) ReadFile(ctx context.Context, bucket string, file string, initialBufferSize int, maxBufferSize int) (<-chan string, <-chan error) {
	// Channels to return the file contents and any potential errors.
	out := make(chan string)
	errChan := make(chan error)

	go func() {
		// Always close the output channel when done.
		defer close(out)

		// Log start of file processing.
		c.Logger.Info("Started processing file: %s from bucket: %s", file, bucket)

		// Get the specified file from the S3 bucket.
		resp, err := c.Svc.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &file,
		})

		if err != nil {
			// On error, send to error channel and exit.
			errChan <- err
			return
		}
		// Ensure the file's body stream is closed when done.
		// Close the filename body when the function exits
		defer func(Body io.ReadCloser) {
			err = Body.Close()
			if err != nil {
				// Send the error to the error channel
				errChan <- err
				return
			}
		}(resp.Body)

		// Get a reader for the file based on its format/type.
		reader, err := GetFileReader(file)(resp.Body)
		if err != nil {
			// On error, send to error channel and exit.
			errChan <- err
			return
		}
		// Ensure the reader is closed when done.
		defer func(reader io.ReadCloser) {
			err := reader.Close()
			if err != nil {
				errChan <- err
				return
			}
		}(reader)

		// Create a scanner to read the file contents.
		scanner := bufio.NewScanner(reader)

		// Initialize a buffer for the scanner, setting its initial and maximum sizes.
		buf := make([]byte, 0, initialBufferSize)
		scanner.Buffer(buf, maxBufferSize)

		// Read the file line by line.
		for scanner.Scan() {
			out <- scanner.Text()
		}

		// Check for any scanning errors.
		if err := scanner.Err(); err != nil {
			// If the error is due to a line being too long, log a specific message.
			if errors.Is(err, bufio.ErrTooLong) {
				c.Logger.Error("Encountered a line that was too long to read in file: %s from bucket: %s, exceeds > %d", file, bucket, maxBufferSize)
			}

			// Send the error to the error channel and exit.
			errChan <- err
			return
		}

		// Log completion of file processing.
		c.Logger.Info("Completed processing of file: %s on bucket: %s", file, bucket)
	}()

	// Return channels to the caller.
	return out, errChan
}

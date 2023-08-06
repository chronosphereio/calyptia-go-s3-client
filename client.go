package s3client

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bmatcuk/doublestar"
	"github.com/calyptia/go-s3-client/ifaces"
)

type (

	// Logger interface to represent a fluent-bit logging mechanism.
	Logger interface {
		Error(format string, a ...any)
		Warn(format string, a ...any)
		Info(format string, a ...any)
		Debug(format string, a ...any)
	}

	// Client is the interface for interacting with an S3 bucket.
	Client interface {
		ListFiles(ctx context.Context, bucket, pattern string) ([]string, error)
		ReadFiles(ctx context.Context, bucket string, files []string, concurrency int, ch chan<- any) error
	}
	// DefaultClient is a concrete implementation of the Client interface that uses the AWS SDK for Go to interact with S3.
	DefaultClient struct {
		Client
		Svc    ifaces.Client
		Logger Logger
	}

	// Message is the msg format as specified by the plugin library (https://github.com/calyptia/plugin/blob/785e54918feb3efb78f9ddeadf135dc4f75fa5b0/plugin.go#L77C1-L81C2)
	Message struct {
		Time   time.Time
		Record any
	}
)

// New returns a new DefaultClient configured with the given options and using the provided logger.
func New(ctx context.Context, logger Logger, optsFns ...ClientOptsFunc) (*DefaultClient, error) {
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

		c.Logger.Info("listing files on bucket: %q with prefix: %q that follows pattern: %q", bucket, prefix, pattern)
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
		c.Logger.Info("found: %d file(s) on bucket: %q that follows pattern: %q", len(files), bucket, pattern)
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

// ReadFiles reads a list of files from the specified bucket and sends their contents
// through the provided channel.
//
// The function takes in a context object to cancel long-running operations, the name
// of the bucket, a slice of strings representing the names of the files to read, and
// a channel of plugin.Message to send the contents of the files.
//
// The function returns an error if there is a problem reading the files or sending
// the messages.
func (c *DefaultClient) ReadFiles(ctx context.Context, bucket string, files []string, concurrency int, ch chan<- any) error {
	// Create a done channel to signal when all the files have been processed
	done := make(chan bool)
	// Create an error channel to handle any errors that occur while processing the files
	errChan := make(chan error)

	// isCritical is a helper function to determine if an error is critical and should cause processing to stop
	isCritical := func(err error) bool {
		// TODO: add logic to determine if an error is critical
		return false
	}

	var wg sync.WaitGroup

	if concurrency == 0 {
		concurrency = 1
	}
	// TODO: make this configurable.
	poolSize := concurrency

	// Create a semaphore to limit the number of files being processed concurrently
	semaphore := make(chan struct{}, poolSize)

	// Iterate over the files and process them concurrently
	for _, file := range files {
		// Acquire a slot in the semaphore
		semaphore <- struct{}{}
		// Add a waiting group for the file being processed
		wg.Add(1)

		go func(filename string) {
			// Release the slot in the semaphore and decrement the waiting group when the function exits
			defer func() {
				wg.Done()
				<-semaphore
			}()

			// Log that the filename processing has started
			c.Logger.Info("started processing filename: %s from bucket: %s", filename, bucket)
			// Get the filename from the S3 bucket
			resp, err := c.Svc.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &filename,
			})
			if err != nil {
				// Send the error to the error channel
				errChan <- err
				return
			}
			// Close the filename body when the function exits
			defer func(Body io.ReadCloser) {
				err = Body.Close()
				if err != nil {
					// Send the error to the error channel
					errChan <- err
					return
				}
			}(resp.Body)

			c.Logger.Info("getting a filename reader for filename: %q and content-type %q", filename, *resp.ContentType)

			// Get a reader for the filename based on its content type
			reader, err := GetFileReader(filename)(resp.Body)
			if err != nil {
				// Send the error to the error channel
				errChan <- err
				return
			}
			defer func() {
				err := reader.Close()
				if err != nil {
					c.Logger.Error("error closing reader fp: %w", err)
				}
			}()

			// Use a buffered reader to read the filename line by line
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				// Send each line of the filename as a message on the channel
				ch <- Message{
					Time: time.Now(),
					Record: map[string]string{
						"_raw":   scanner.Text(),
						"bucket": bucket,
						"file":   filename,
					},
				}
			}
			// If there is an error with the scanner, send the error to the errChan
			// channel and return from the function.
			if err := scanner.Err(); err != nil {
				errChan <- err
				return
			}
			c.Logger.Info("completed processing of filename: %s on bucket: %s", filename, bucket)
		}(file)
	}

	// Start a goroutine that waits for all the file processing goroutines to finish.
	// When they are all done, send a message on the done channel.
	go func() {
		wg.Wait()
		done <- true
	}()

	// Loop indefinitely until the context is done or the done channel is closed.
	for {
		select {
		case <-ctx.Done():
			// If the context is done, return the context's error.
			return ctx.Err()
		case <-done:
			// If the done channel is closed, close the done channel and return nil.
			close(done)
			return nil
		case err := <-errChan:
			// If there is an error on the errChan channel, log it and check if it is critical.
			// If it is critical, return the error.
			c.Logger.Error("error while processing file from s3 bucket:%w", err)
			if isCritical(err) {
				return err
			}
		}
	}
}

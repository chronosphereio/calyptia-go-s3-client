package s3client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"

	"github.com/calyptia/go-s3-client/ifaces"
)

type NullLogger struct{}

func (n NullLogger) Error(format string, a ...any) {}
func (n NullLogger) Warn(format string, a ...any)  {}
func (n NullLogger) Info(format string, a ...any)  {}
func (n NullLogger) Debug(format string, a ...any) {}

func TestDefaultClient_ReadFile(t *testing.T) {
	ctx := context.TODO()

	t.Run("ok", func(t *testing.T) {
		tt := []*struct {
			name             string
			clientMock       *ifaces.ClientMock
			file             string
			expectedErr      error
			bucket           string
			expectedMessages func() []string
		}{
			{
				name: "single line",
				file: "single-line-file.txt",
				clientMock: &ifaces.ClientMock{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						return &s3.GetObjectOutput{
							Body:        io.NopCloser(bytes.NewReader([]byte("single line"))),
							ContentType: aws.String("text/csv"),
						}, nil
					},
				},
				expectedMessages: func() []string {
					return []string{"single line"}
				},
			},
			{
				name: "compressed multiline",
				file: "testdata/large-file.csv.gz",
				clientMock: &ifaces.ClientMock{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						pwd, err := os.Getwd()
						assert.NoError(t, err)

						filename := filepath.Join(pwd, "testdata/large-file.csv.gz")
						// nolint:gosec //ignore this is just a test.
						content, err := os.ReadFile(filename)
						assert.NoError(t, err)
						return &s3.GetObjectOutput{
							Body:        io.NopCloser(bytes.NewReader(content)),
							ContentType: aws.String("application/octet-stream"),
						}, nil
					},
				},
				expectedMessages: func() []string {
					// assuming a helper function to read file content
					lines, err := linesFromFile("testdata/large-file.csv.gz")
					assert.NoError(t, err)
					return lines
				},
			},
			{
				name: "compressed with invalid content type",
				file: "testdata/large-file-invalid-content-type.csv.gz",
				clientMock: &ifaces.ClientMock{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						pwd, err := os.Getwd()
						assert.NoError(t, err)

						filename := filepath.Join(pwd, "testdata/large-file-invalid-content-type.csv.gz")
						// nolint:gosec //ignore this is just a test.
						content, err := os.ReadFile(filename)
						assert.NoError(t, err)
						return &s3.GetObjectOutput{
							Body:        io.NopCloser(bytes.NewReader(content)),
							ContentType: aws.String("text/csv"),
						}, nil
					},
				},
				expectedMessages: func() []string {
					lines, err := linesFromFile("testdata/large-file-invalid-content-type.csv.gz")
					assert.NoError(t, err)
					return lines
				},
			},
			{
				name: "line larger than 10MiB",
				file: "very-large-line.txt",
				clientMock: &ifaces.ClientMock{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						// Creating a string with more than 10MiB length.
						veryLongLine := strings.Repeat("a", 11*1024*1024) // 11 MiB

						return &s3.GetObjectOutput{
							Body:        io.NopCloser(bytes.NewReader([]byte(veryLongLine))),
							ContentType: aws.String("text/csv"),
						}, nil
					},
				},
				expectedErr:      bufio.ErrTooLong,               // Expecting an error in this case
				expectedMessages: func() []string { return nil }, // No valid messages to expect here
			},
		}

		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				c := DefaultClient{
					Svc:    tc.clientMock,
					Logger: NullLogger{},
				}

				withTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
				defer cancel()

				outCh, errCh := c.ReadFile(withTimeout, tc.bucket, tc.file, 64*1024, 10*1024*1024)

				expectedMessages := tc.expectedMessages()
				idx := 0
				for {
					select {
					case line := <-outCh:
						// Ensure we don't go beyond the expected messages length
						if expectedMessages != nil && idx < len(expectedMessages) {
							assert.Equal(t, line, expectedMessages[idx], "Mismatch at index %d", idx)
							idx++
						}
					case err := <-errCh:
						assert.Equal(t, err, tc.expectedErr)
						return
					case <-withTimeout.Done():
						return
					}
				}
			})
		}
	})
}

func linesFromFile(filePath string) ([]string, error) {
	var out []string
	pwd, err := os.Getwd()
	if err != nil {
		return out, err
	}

	path := filepath.Join(pwd, filePath)

	//nolint gosec // this is a test, ignore
	file, err := os.Open(path)
	if err != nil {
		return out, err
	}

	reader, err := GetFileReader(path)(bufio.NewReader(file))
	if err != nil {
		return out, err
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func TestDefaultClient_ListFiles(t *testing.T) {
	ctx := context.TODO()

	t.Run("ok", func(t *testing.T) {
		client := ifaces.ClientMock{
			ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{
					Contents: []types.Object{
						{
							Key:          aws.String("one"),
							LastModified: aws.Time(time.Now()),
						},
					},
					KeyCount: aws.Int32(1),
					MaxKeys:  aws.Int32(1000),
					Name:     aws.String("mock"),
					Prefix:   aws.String("prefix"),
				}, nil
			},
		}

		c := DefaultClient{
			Svc:    &client,
			Logger: NullLogger{},
		}

		files, err := c.ListFiles(context.TODO(), "", "**/one")
		assert.NoError(t, err)
		assert.NotEqual(t, len(files), 0)
	})

	t.Run("ok multiple", func(t *testing.T) {
		client := ifaces.ClientMock{
			ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{
					Contents: []types.Object{
						{
							Key:          aws.String("one.log"),
							LastModified: aws.Time(time.Now()),
						},
						{
							Key:          aws.String("two.log"),
							LastModified: aws.Time(time.Now()),
						},
					},
					KeyCount: aws.Int32(2),
					MaxKeys:  aws.Int32(1000),
					Name:     aws.String("mock"),
					Prefix:   aws.String("prefix"),
				}, nil
			},
		}

		c := DefaultClient{
			Svc:    &client,
			Logger: NullLogger{},
		}

		files, err := c.ListFiles(ctx, "", "*.log")
		assert.NoError(t, err)
		assert.Equal(t, len(files), 2)
	})

	t.Run("no match", func(t *testing.T) {
		client := ifaces.ClientMock{
			ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{}, nil
			},
		}

		c := DefaultClient{
			Svc:    &client,
			Logger: NullLogger{},
		}

		files, err := c.ListFiles(ctx, "", "*.log")
		assert.NoError(t, err)
		assert.Zero(t, len(files))
	})

	t.Run("error", func(t *testing.T) {
		client := ifaces.ClientMock{
			ListObjectsV2Func: func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				return &s3.ListObjectsV2Output{}, fmt.Errorf("cannot retrieve objects")
			},
		}

		c := DefaultClient{
			Svc:    &client,
			Logger: NullLogger{},
		}

		files, err := c.ListFiles(ctx, "", "*.log")
		assert.Error(t, err)
		assert.Zero(t, files)
		assert.EqualError(t, err, "error listing files from s3: cannot retrieve objects")
	})
}

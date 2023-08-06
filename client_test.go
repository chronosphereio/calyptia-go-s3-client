package s3client

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func msgsFromFile(filePath string) ([]Message, error) {
	var out []Message

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

	var msgs []Message
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		msgs = append(msgs, Message{
			Record: map[string]string{
				"_raw": scanner.Text(),
				"file": path,
			},
		})
	}

	return msgs, nil
}

func TestDefaultClient_ReadFiles(t *testing.T) {
	ctx := context.TODO()

	t.Run("ok", func(t *testing.T) {
		tt := []struct {
			name             string
			clientMock       ifaces.ClientMock
			files            []string
			channel          chan Message
			expectedErr      error
			expectedMessages func() []Message
			bucket           string
		}{
			{
				name: "single line",
				files: []string{
					"single-line-file.txt",
				},
				clientMock: ifaces.ClientMock{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						return &s3.GetObjectOutput{
							Body:        io.NopCloser(bytes.NewReader([]byte("single line"))),
							ContentType: aws.String("text/csv"),
						}, nil
					},
				},
				channel: make(chan Message),
				expectedMessages: func() []Message {
					return []Message{{
						Record: map[string]string{
							"_raw": "single line",
							"file": "single-line-file.txt",
						},
					}}
				},
			},
			{
				name: "compressed multiline",
				files: []string{
					"testadata/large-file.csv.gz",
				},
				clientMock: ifaces.ClientMock{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						pwd, err := os.Getwd()
						assert.NoError(t, err)

						filename := filepath.Join(pwd, "testdata/large-file.csv.gz")
						//nolint gosec // this is a test, ignore
						content, err := os.ReadFile(filename)
						assert.NoError(t, err)
						return &s3.GetObjectOutput{
							Body:        io.NopCloser(bytes.NewReader(content)),
							ContentType: aws.String("application/octet-stream"),
						}, nil
					},
				},
				channel: make(chan Message),
				expectedMessages: func() []Message {
					msgs, err := msgsFromFile("testdata/large-file.csv.gz")
					assert.NoError(t, err)
					assert.NotZero(t, msgs)
					return msgs
				},
			},
			{
				name: "compressed with invalid content type",
				files: []string{
					"testdata/large-file-invalid-content-type.csv.gz",
				},
				clientMock: ifaces.ClientMock{
					GetObjectFunc: func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
						pwd, err := os.Getwd()
						assert.NoError(t, err)

						filename := filepath.Join(pwd, "testdata/large-file-invalid-content-type.csv.gz")
						//nolint gosec // this is a test, ignore
						content, err := os.ReadFile(filename)
						assert.NoError(t, err)
						return &s3.GetObjectOutput{
							Body:        io.NopCloser(bytes.NewReader(content)),
							ContentType: aws.String("text/csv"),
						}, nil
					},
				},
				channel: make(chan Message),
				expectedMessages: func() []Message {
					msgs, err := msgsFromFile("testdata/large-file-invalid-content-type.csv.gz")
					assert.NoError(t, err)
					assert.NotZero(t, msgs)
					return msgs
				},
			},
		}

		//nolint copylocks //that's fine.
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				c := DefaultClient{
					Svc:    &tc.clientMock,
					Logger: NullLogger{},
				}

				withTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				idx := 0
				go func() {
					expectedMessages := tc.expectedMessages()
					for {
						select {
						case msg := <-tc.channel:
							receivedRecord, ok := msg.Record.(map[string]string)
							assert.NotZero(t, ok)
							expectedRecord, ok := expectedMessages[idx].Record.(map[string]string)
							assert.NotZero(t, ok)
							assert.Equal(t, receivedRecord["_raw"], expectedRecord["_raw"])
							assert.Equal(t, filepath.Base(receivedRecord["file"]), filepath.Base(expectedRecord["file"]))
							idx++
						case <-withTimeout.Done():
							return
						}
					}
				}()

				err := c.ReadFiles(withTimeout, tc.bucket, tc.files, 0, tc.channel)
				assert.Equal(t, err, tc.expectedErr)
				t.Logf("processed: %d messages for test case: %s", idx, tc.name)
			})
		}
	})
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
					KeyCount: 1,
					MaxKeys:  1000,
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
					KeyCount: 2,
					MaxKeys:  1000,
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

package s3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// multipartWriter buffers writes in memory and uploads via
// PutObject on Close. v0: single-shot upload. The multipart state
// machine (initiate → upload parts → complete, 5 MiB threshold,
// concurrent part uploads) lands in a follow-up commit — the
// signature is designed to absorb it without changing callers.
//
// For the MVP, parquet parts are already bounded at 128 MiB
// (DefaultTargetBytes), so single-shot PutObject is correct for
// the common case. Multipart matters when a single part exceeds the
// S3 part limit (5 GiB) — rare but real.
type multipartWriter struct {
	ctx    context.Context
	b      *Backend
	key    string
	buffer bytes.Buffer
}

func newMultipartWriter(ctx context.Context, b *Backend, key string) *multipartWriter {
	return &multipartWriter{ctx: ctx, b: b, key: key}
}

func (w *multipartWriter) Write(p []byte) (int, error) {
	return w.buffer.Write(p)
}

func (w *multipartWriter) Close() error {
	body := bytes.NewReader(w.buffer.Bytes())
	_, err := w.b.client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.b.bucket),
		Key:    aws.String(w.key),
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("s3: PutObject s3://%s/%s: %w", w.b.bucket, w.key, err)
	}
	return nil
}

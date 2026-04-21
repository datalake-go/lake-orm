package testutils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	miniomod "github.com/testcontainers/testcontainers-go/modules/minio"
)

// MinIOResult bundles the container handle, credentials, and a
// preconfigured *s3.Client that tests usually want. Returned by
// NewMinIO. Cleanup is already registered against t.
type MinIOResult struct {
	Endpoint  string // http URL — e.g. http://127.0.0.1:54321
	AccessKey string
	SecretKey string
	Client    *awss3.Client
}

// NewMinIO spins up a MinIO testcontainer and returns a bundle the
// test can use directly. The returned MinIOResult carries the
// endpoint + creds so callers can pass them straight into the S3
// DSN parser. Container is terminated when the test finishes — no
// explicit cleanup needed.
func NewMinIO(t *testing.T) *MinIOResult {
	t.Helper()
	ctx := context.Background()

	// Pinned image tag so test runs are reproducible across machines
	// and CI runners. Bump deliberately when MinIO ships relevant
	// S3-compatibility fixes.
	ctr, err := miniomod.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminate minio container: %v", err)
		}
	})

	endpoint, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("get minio endpoint: %v", err)
	}
	if !strings.HasPrefix(endpoint, "http") {
		endpoint = "http://" + endpoint
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		// us-east-1 is the region MinIO reports in its canonicalization
		// path — using anything else triggers a signature mismatch.
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			ctr.Username, ctr.Password, "",
		)),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}

	client := awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		// Path-style addressing is required for every S3-compatible
		// service that isn't real AWS — MinIO, SeaweedFS, LocalStack,
		// Ceph. Virtual-hosted URLs require DNS for the bucket.
		o.UsePathStyle = true
	})

	return &MinIOResult{
		Endpoint:  endpoint,
		AccessKey: ctr.Username,
		SecretKey: ctr.Password,
		Client:    client,
	}
}

// DSN renders the MinIO bundle as a DORM S3 DSN pointing at `bucket`.
// Ready to pass to backend.S3(...).
func (m *MinIOResult) DSN(bucket, prefix string) string {
	return fmt.Sprintf(
		"s3://%s/%s?endpoint=%s&path_style=true&access_key=%s&secret_key=%s",
		bucket, prefix, m.Endpoint, m.AccessKey, m.SecretKey,
	)
}

// NewBucket creates a uniquely-named bucket on MinIO and registers
// cleanup to empty and delete it when the test finishes. Returns the
// bucket name. Names are 3-63 chars, lowercase alphanumerics and
// hyphens — the "test-<nanos>" prefix stays well under the limit.
func (m *MinIOResult) NewBucket(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	bucket := fmt.Sprintf("test-%d", time.Now().UnixNano())

	_, err := m.Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		// Idempotent: if we've already created it (unlikely given the
		// ns-precision suffix, but defensive against retries), don't
		// fail the test.
		var owned *s3types.BucketAlreadyOwnedByYou
		if !errors.As(err, &owned) {
			t.Fatalf("create bucket %s: %v", bucket, err)
		}
	}
	t.Cleanup(func() { m.emptyAndDelete(t, bucket) })
	return bucket
}

// PutObject uploads a test object.
func (m *MinIOResult) PutObject(t *testing.T, bucket, key string, body []byte) {
	t.Helper()
	_, err := m.Client.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("put object %s/%s: %v", bucket, key, err)
	}
}

// GetObject fetches and returns an object's bytes.
func (m *MinIOResult) GetObject(t *testing.T, bucket, key string) []byte {
	t.Helper()
	out, err := m.Client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("get object %s/%s: %v", bucket, key, err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read body %s/%s: %v", bucket, key, err)
	}
	return body
}

// ListKeys lists all keys in bucket under prefix, in lexical order.
// S3/MinIO return ListObjectsV2 results sorted by key, so the slice
// ordering is stable.
func (m *MinIOResult) ListKeys(t *testing.T, bucket, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	var keys []string
	var token *string
	for {
		out, err := m.Client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			t.Fatalf("list objects %s/%s: %v", bucket, prefix, err)
		}
		for _, obj := range out.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return keys
}

func (m *MinIOResult) emptyAndDelete(t *testing.T, bucket string) {
	t.Helper()
	ctx := context.Background()
	// Best-effort cleanup — the container goes away anyway so we log
	// rather than fail a test that was otherwise successful.
	keys := m.ListKeys(t, bucket, "")
	for _, key := range keys {
		_, err := m.Client.DeleteObject(ctx, &awss3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil && !strings.Contains(err.Error(), "NoSuchKey") {
			t.Logf("cleanup: delete %s/%s: %v", bucket, key, err)
		}
	}
	if _, err := m.Client.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Logf("cleanup: delete bucket %s: %v", bucket, err)
	}
}

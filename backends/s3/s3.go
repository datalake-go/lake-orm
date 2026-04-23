// Package s3 provides an S3-compatible Backend built directly on
// aws-sdk-go-v2/service/s3. Works with real AWS S3, Cloudflare R2,
// SeaweedFS, MinIO, LocalStack, and Ceph via query-string-configured
// endpoints.
//
// No generic storage-abstraction layer sits between this package and
// the SDK — each backend owns its SDK outright, so multipart upload
// tuning, retry classification, and error handling can be optimized
// for the lakehouse workflow without leaking through a lowest-common
// denominator interface.
package s3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/datalake-go/lake-orm/types"
)

// Backend is the S3 implementation of backends.Backend.
type Backend struct {
	client *s3.Client
	bucket string
	root   string // prefix under bucket where the lakehouse lives
	region string
}

// New constructs an S3 backend from a DSN. See the package doc for the
// grammar; common forms:
//
//	backends.S3("s3://my-bucket/lake")
//	backends.S3("s3://lakeorm-local/lake?endpoint=http://localhost:8333&path_style=true&access_key=lakeorm&secret_key=lakeorm")
//	backends.S3("s3://my-bucket/lake?endpoint=https://<acct>.r2.cloudflarestorage.com&path_style=true")
func New(dsn string) (*Backend, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("s3 backend: parse DSN %q: %w", dsn, err)
	}
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("s3 backend: expected scheme s3, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("s3 backend: missing bucket in DSN %q", dsn)
	}

	q := u.Query()

	region := q.Get("region")
	if region == "" {
		region = os.Getenv("AWS_REGION")
	}
	if region == "" {
		region = "us-east-1"
	}

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}

	// Explicit creds in the DSN override the resolution chain. This is
	// convenient for lakeorm-local; production deployments should use
	// IRSA / instance profiles / env vars and leave these empty.
	ak := q.Get("access_key")
	sk := q.Get("secret_key")
	st := q.Get("session_token")
	if ak != "" && sk != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(ak, sk, st),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("s3 backend: load AWS config: %w", err)
	}

	clientOpts := []func(*s3.Options){}

	if ep := q.Get("endpoint"); ep != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(ep)
		})
	}
	if ps, _ := strconv.ParseBool(q.Get("path_style")); ps {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(cfg, clientOpts...)

	return &Backend{
		client: client,
		bucket: u.Host,
		root:   strings.TrimPrefix(u.Path, "/"),
		region: region,
	}, nil
}

func (b *Backend) Name() string { return "s3" }

func (b *Backend) RootURI() string {
	if b.root == "" {
		return "s3://" + b.bucket
	}
	return "s3://" + b.bucket + "/" + b.root
}

func (b *Backend) TableLocation(tableName string) types.Location {
	return types.Location{
		Scheme: "s3",
		Bucket: b.bucket,
		Path:   joinPath(b.root, "tables", tableName),
	}
}

func (b *Backend) StagingPrefix(ingestID string) string {
	return joinPath(b.root, "staging", ingestID)
}

// StagingLocation returns the absolute URI Spark needs to read a
// staging prefix via Hadoop-AWS. The scheme is s3a:// (not s3://)
// because s3:// is registered to a different Hadoop filesystem
// driver that ships with MapReduce and doesn't understand our
// config — s3a:// is the one Hadoop-AWS claims. Same bucket + key,
// different protocol registration.
func (b *Backend) StagingLocation(ingestID string) types.Location {
	return types.Location{
		Scheme: "s3a",
		Bucket: b.bucket,
		Path:   b.StagingPrefix(ingestID),
	}
}

func (b *Backend) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	// v0 buffers the object in memory and PutObject on close. The
	// multipart-upload state machine lives in multipart.go and takes
	// over in a follow-up commit (5 MiB threshold, 5 MiB part size —
	// matches AWS SDK recommendations for parquet parts).
	return newMultipartWriter(ctx, b, key), nil
}

func (b *Backend) Reader(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3: GetObject s3://%s/%s: %w", b.bucket, key, err)
	}
	return out.Body, nil
}

func (b *Backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3: DeleteObject s3://%s/%s: %w", b.bucket, key, err)
	}
	return nil
}

func (b *Backend) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var contToken *string
	for {
		out, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(b.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: contToken,
		})
		if err != nil {
			return nil, fmt.Errorf("s3: ListObjectsV2 s3://%s/%s: %w", b.bucket, prefix, err)
		}
		for _, obj := range out.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		contToken = out.NextContinuationToken
	}
	return keys, nil
}

func (b *Backend) CleanupStaging(ctx context.Context, prefix string) error {
	keys, err := b.List(ctx, prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := b.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

func joinPath(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.Trim(p, "/")
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, "/")
}

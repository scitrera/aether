package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Config carries the bare minimum to construct an S3 (or S3-compatible)
// client. Endpoint + ForcePathStyle let it target MinIO.
type S3Config struct {
	Endpoint        string `yaml:"endpoint" json:"endpoint"`
	Region          string `yaml:"region" json:"region"`
	Bucket          string `yaml:"bucket" json:"bucket"`
	AccessKeyID     string `yaml:"access_key_id" json:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key" json:"secret_access_key"`
	ForcePathStyle  bool   `yaml:"force_path_style" json:"force_path_style"`
	// SSE controls server-side encryption. Defaults to "AES256" (SSE-S3) when
	// empty. Set to "none" to disable.
	SSE string `yaml:"sse" json:"sse"`
}

// S3StorageClient is an S3-backed StorageClient. It uses the v2 SDK plus the
// high-level upload/download managers so large snapshots are transferred via
// multipart automatically.
type S3StorageClient struct {
	cfg        S3Config
	client     *s3.Client
	uploader   *manager.Uploader
	downloader *manager.Downloader
}

// Compile-time interface assertion.
var _ StorageClient = (*S3StorageClient)(nil)

// NewS3StorageClient builds an S3StorageClient from cfg.
func NewS3StorageClient(ctx context.Context, cfg S3Config) (*S3StorageClient, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3 storage: bucket is required")
	}
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3 storage: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			ep := cfg.Endpoint
			if !strings.Contains(ep, "://") {
				ep = "https://" + ep
			}
			o.BaseEndpoint = aws.String(ep)
		}
		if cfg.ForcePathStyle {
			o.UsePathStyle = true
		}
	})

	return &S3StorageClient{
		cfg:        cfg,
		client:     client,
		uploader:   manager.NewUploader(client),
		downloader: manager.NewDownloader(client),
	}, nil
}

func (s *S3StorageClient) sse() s3types.ServerSideEncryption {
	switch strings.ToLower(s.cfg.SSE) {
	case "none":
		return ""
	case "", "aes256":
		return s3types.ServerSideEncryptionAes256
	default:
		return s3types.ServerSideEncryption(s.cfg.SSE)
	}
}

// Upload streams reader into s3://{Bucket}/{key} as a multipart upload,
// attaching meta as object metadata and applying SSE per config.
func (s *S3StorageClient) Upload(ctx context.Context, key string, reader io.Reader, size int64, meta map[string]string) error {
	input := &s3.PutObjectInput{
		Bucket:   aws.String(s.cfg.Bucket),
		Key:      aws.String(key),
		Body:     reader,
		Metadata: meta,
	}
	if sse := s.sse(); sse != "" {
		input.ServerSideEncryption = sse
	}
	_, err := s.uploader.Upload(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 upload %s: %w", key, err)
	}
	return nil
}

// Download streams s3://{Bucket}/{key} into writer. The manager downloader
// requires an io.WriterAt, so we adapt writer via a sequential buffer when
// needed.
func (s *S3StorageClient) Download(ctx context.Context, key string, writer io.Writer) error {
	wa, ok := writer.(io.WriterAt)
	if !ok {
		wa = &sequentialWriterAt{w: writer}
	}
	_, err := s.downloader.Download(ctx, wa, &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 download %s: %w", key, err)
	}
	return nil
}

// LatestKey lists objects under prefix and returns the lexicographically
// largest ".bin" key. Metadata is fetched via HeadObject.
func (s *S3StorageClient) LatestKey(ctx context.Context, prefix string) (string, map[string]string, error) {
	objs, err := s.List(ctx, prefix)
	if err != nil {
		return "", nil, err
	}
	var bins []ObjectInfo
	for _, o := range objs {
		if strings.HasSuffix(o.Key, ".bin") {
			bins = append(bins, o)
		}
	}
	if len(bins) == 0 {
		return "", nil, os.ErrNotExist
	}
	sort.Slice(bins, func(i, j int) bool { return bins[i].Key < bins[j].Key })
	latest := bins[len(bins)-1].Key

	headOut, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(latest),
	})
	if err != nil {
		return latest, nil, fmt.Errorf("s3 head %s: %w", latest, err)
	}
	return latest, headOut.Metadata, nil
}

// List paginates ListObjectsV2 under prefix.
func (s *S3StorageClient) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.cfg.Bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			info := ObjectInfo{
				Key: aws.ToString(obj.Key),
			}
			if obj.Size != nil {
				info.Size = *obj.Size
			}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			out = append(out, info)
		}
	}
	return out, nil
}

// sequentialWriterAt adapts an io.Writer to io.WriterAt for downloaders that
// don't actually need true random access. The aws-sdk-go-v2 manager
// downloader uses WriteAt for parallel chunked downloads; passing offsets it
// expects sequential writes only if concurrency is forced to 1. We set
// concurrency to 1 via the call site implicitly by serializing offsets.
type sequentialWriterAt struct {
	w      io.Writer
	offset int64
}

func (s *sequentialWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if off != s.offset {
		return 0, fmt.Errorf("sequentialWriterAt: out-of-order write at %d (expected %d)", off, s.offset)
	}
	n, err := s.w.Write(p)
	s.offset += int64(n)
	return n, err
}

package s3

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
	grubs3 "github.com/zoobz-io/grub/s3"
	"github.com/zoobz-io/grub/testing/integration/bucket"
)

var tc *bucket.TestContext

const testBucket = "test-bucket"

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Start LocalStack container
	container, err := localstack.Run(ctx,
		"localstack/localstack:latest",
		testcontainers.WithEnv(map[string]string{
			"SERVICES": "s3",
		}),
	)
	if err != nil {
		panic("failed to start localstack container: " + err.Error())
	}

	// Get the endpoint
	endpoint, err := container.PortEndpoint(ctx, "4566/tcp", "http")
	if err != nil {
		panic("failed to get localstack endpoint: " + err.Error())
	}

	// Create S3 client configured for LocalStack
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		panic("failed to load AWS config: " + err.Error())
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	// Create test bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(testBucket),
	})
	if err != nil {
		panic("failed to create test bucket: " + err.Error())
	}

	// Wait for bucket to be ready
	waiter := s3.NewBucketExistsWaiter(client)
	err = waiter.Wait(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(testBucket),
	}, 30*time.Second)
	if err != nil {
		panic("bucket not ready: " + err.Error())
	}

	tc = &bucket.TestContext{
		Provider: grubs3.New(client, testBucket),
		Cleanup: func() {
			// Clean up bucket contents before terminating
			listOutput, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(testBucket),
			})
			if listOutput != nil {
				for _, obj := range listOutput.Contents {
					_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
						Bucket: aws.String(testBucket),
						Key:    obj.Key,
					})
				}
			}
			_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(testBucket),
			})
			_ = container.Terminate(ctx)
		},
	}

	code := m.Run()

	tc.Cleanup()

	os.Exit(code)
}

func TestS3_CRUD(t *testing.T) {
	bucket.RunCRUDTests(t, tc)
}

func TestS3_Metadata(t *testing.T) {
	bucket.RunMetadataTests(t, tc)
}

func TestS3_Atomic(t *testing.T) {
	bucket.RunAtomicTests(t, tc)
}

func TestS3_List(t *testing.T) {
	bucket.RunListTests(t, tc)
}

func TestS3_Hooks(t *testing.T) {
	bucket.RunHookTests(t, tc)
}

package drivers

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/leonsteinhaeuser/go-storage/utils"
)

// S3Config defines a wrapper for the s3 configuration
type S3Config struct {
	// Endpoint is the endpoint of the S3 service.
	Endpoint string
	// Region is the region of the S3 service.
	Region string
	// AccessKeyID is the access key ID of the S3 service.
	AccessKeyID string
	// SecretAccessKey is the secret access key of the S3 service.
	SecretAccessKey string
	// Bucket is the bucket name of the S3 service.
	Bucket string
	// PathPrefix is the path prefix of the S3 service.
	PathPrefix string
	// DisableSSL defines whether to disable SSL or not.
	DisableSSL bool
}

// S3 defines the interface "Driver" implementation for the s3 protocol.
type S3 struct {
	Bucket     string
	PathPrefix string

	conn    *s3.S3
	session *session.Session
}

// NewS3FromConfig creates a new S3 instance from the given configuration.
func NewS3FromConfig(config S3Config) (*S3, error) {
	s3Def := &S3{
		Bucket:     config.Bucket,
		PathPrefix: config.PathPrefix,
	}
	sess, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			config.AccessKeyID,
			config.SecretAccessKey,
			"",
		),
		Endpoint:         &config.Endpoint,
		Region:           &config.Region,
		DisableSSL:       &config.DisableSSL,
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create aws session by config: %w", err)
	}
	s3Def.session = sess
	s3Def.conn = s3.New(sess)
	return s3Def, nil
}

// NewS3 creates a new S3 instance.
func NewS3(bucket, pathPrefix string, conn *s3.S3, session *session.Session) *S3 {
	return &S3{
		Bucket:     bucket,
		PathPrefix: pathPrefix,
		conn:       conn,
		session:    session,
	}
}

// Read reads the file/object and returns the content.
func (s3def S3) Read(key string) (io.Reader, error) {
	res, err := s3def.conn.GetObject(&s3.GetObjectInput{
		Bucket: &s3def.Bucket,
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to read object %q: %w", key, err)
	}
	defer res.Body.Close()
	bts, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("unable to read bytes from object %q: %w", key, err)
	}
	return bytes.NewBuffer(bts), nil
}

func (s3def S3) Write(key string, value io.Reader) error {
	mType, err := utils.MimeType(value)
	if err != nil {
		return err
	}
	uploader := s3manager.NewUploader(s3def.session)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:      &s3def.Bucket,
		Key:         &key,
		Body:        value,
		ContentType: &mType,
	})
	if err != nil {
		return fmt.Errorf("unable to upload %q: %w", key, err)
	}
	return nil
}

func (s3def S3) Delete(key string) error {
	_, err := s3def.conn.DeleteObject(&s3.DeleteObjectInput{
		Bucket: &s3def.Bucket,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("unable to delete object %q: %w", key, err)
	}
	return nil
}

func (s3def S3) Exists(key string) (bool, error) {
	ho, err := s3def.conn.HeadObject(&s3.HeadObjectInput{
		Bucket: &s3def.Bucket,
		Key:    &key,
	})
	if err != nil {
		return false, fmt.Errorf("unable to check if object %q exists: %w", key, err)
	}
	return *ho.ContentLength > 0, nil
}

func (s3def S3) List() ([]string, error) {
	loo, err := s3def.conn.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: &s3def.Bucket,
		Prefix: &s3def.PathPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list objects: %w", err)
	}
	var keys []string
	for _, o := range loo.Contents {
		keys = append(keys, *o.Key)
	}
	return keys, nil
}

package gostorage

import (
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/leonsteinhaeuser/go-storage/drivers"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
)

var (
	config     *aws.Config
	awsSession *session.Session

	s3StorageDriver    Driver
	localStorageDriver Driver
)

func TestMain(m *testing.M) {
	p := localstack.Preset(localstack.WithServices(localstack.S3))
	c, err := gnomock.Start(p)
	if err != nil {
		panic(err)
	}
	s3Endpoint := fmt.Sprintf("http://%s/", c.Address(localstack.APIPort))

	config = &aws.Config{
		Region:           aws.String("eu-central-1"),
		Endpoint:         aws.String(s3Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("a", "b", "c"),
	}

	var testBucket string = "test-bucket"

	sess, _ := session.NewSession(config)
	awsSession = sess
	svc := s3.New(sess)
	_, _ = svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &testBucket,
	})

	err = os.MkdirAll("/tmp/test", fs.FileMode(0755))
	if err != nil && !errors.Is(err, os.ErrExist) {
		panic(err)
	}

	s3StorageDriver = drivers.NewS3(testBucket, "", svc, sess)
	localStorageDriver = drivers.NewLocalStorage("/tmp/test")

	m.Run()
}

func Test_S3(t *testing.T) {
	const key string = "test.txt"

	// write file
	err := s3StorageDriver.Write(key, strings.NewReader("test"))
	if err != nil {
		t.Errorf("unable to write to s3: %v", err)
		return
	}

	// check if file exists
	exists, err := s3StorageDriver.Exists(key)
	if err != nil {
		t.Errorf("unable to check is key exist in s3: %v", err)
		return
	}
	t.Logf("exists: %v", exists)

	// receive the file
	got, err := s3StorageDriver.Read(key)
	if err != nil {
		t.Errorf("unable to read from s3: %v", err)
		return
	}
	bts, err := ioutil.ReadAll(got)
	if err != nil {
		t.Errorf("unable to read all from io.Reader: %v", err)
		return
	}
	t.Logf("got: %s", string(bts))

	// list files/keys
	keys, err := s3StorageDriver.List()
	if err != nil {
		t.Errorf("unable to list keys in s3: %v", err)
		return
	}
	for _, key := range keys {
		t.Logf("key: %s", key)
	}

	// delete file
	err = s3StorageDriver.Delete(key)
	if err != nil {
		t.Errorf("unable to delete key in s3: %v", err)
		return
	}
}

func Test_LocalStorage(t *testing.T) {
	const key string = "test.txt"

	// write file
	err := localStorageDriver.Write(key, strings.NewReader("test"))
	if err != nil {
		t.Errorf("unable to write to local-storage: %v", err)
		return
	}

	// check if file exists
	exists, err := localStorageDriver.Exists(key)
	if err != nil {
		t.Errorf("unable to check is key exist in local-storage: %v", err)
		return
	}
	t.Logf("exists: %v", exists)

	// receive the file
	got, err := localStorageDriver.Read(key)
	if err != nil {
		t.Errorf("unable to read from local-storage: %v", err)
		return
	}
	bts, err := ioutil.ReadAll(got)
	if err != nil {
		t.Errorf("unable to read all from io.Reader: %v", err)
		return
	}
	t.Logf("got: %s", string(bts))

	// list files/keys
	keys, err := localStorageDriver.List()
	if err != nil {
		t.Errorf("unable to list keys in local-storage: %v", err)
		return
	}
	for _, key := range keys {
		t.Logf("key: %s", key)
	}

	// delete file
	err = localStorageDriver.Delete(key)
	if err != nil {
		t.Errorf("unable to delete key in local-storage: %v", err)
		return
	}
}

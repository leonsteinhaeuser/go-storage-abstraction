package drivers

import (
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
)

var (
	testBucket string = "test-bucket"

	config     *aws.Config
	awsSession *session.Session
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

	sess, _ := session.NewSession(config)
	awsSession = sess
	svc := s3.New(sess)
	_, _ = svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: &testBucket,
	})

	m.Run()
}

func TestNewS3FromConfig(t *testing.T) {
	type args struct {
		config S3Config
	}
	tests := []struct {
		name    string
		args    args
		want    *S3
		wantErr bool
	}{
		{
			name: "valid config",
			args: args{
				config: S3Config{
					Endpoint:        "http://localhost:4572",
					Region:          "eu-central-1",
					AccessKeyID:     "sample-access-key-id",
					SecretAccessKey: "sample-secret-access-key",
					Bucket:          "sample-bucket",
					PathPrefix:      "",
					DisableSSL:      true,
				},
			},
			want: &S3{
				Bucket:     "sample-bucket",
				PathPrefix: "",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewS3FromConfig(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewS3FromConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got.Bucket != tt.want.Bucket {
				t.Errorf("NewS3FromConfig() Bucket = %v, want %v", got, tt.want)
				return
			}

			if got.PathPrefix != tt.want.PathPrefix {
				t.Errorf("NewS3FromConfig() Bucket = %v, want %v", got, tt.want)
				return
			}

			if (err != nil) != tt.wantErr {
				if got.conn == nil {
					t.Errorf("NewS3FromConfig() conn = %v, want not nil", got)
					return
				}

				if got.session == nil {
					t.Errorf("NewS3FromConfig() conn = %v, want not nil", got)
					return
				}
				return
			}
		})
	}
}

func TestNewS3(t *testing.T) {
	type args struct {
		bucket     string
		pathPrefix string
		conn       *s3.S3
		session    *session.Session
	}
	tests := []struct {
		name            string
		args            args
		want            *S3
		wantNilSessions bool
	}{
		{
			name: "valid config",
			args: args{
				bucket:     testBucket,
				pathPrefix: "",
				conn: func() *s3.S3 {
					s, _ := session.NewSession(config)
					return s3.New(s)
				}(),
				session: func() *session.Session {
					s, _ := session.NewSession(config)
					return s
				}(),
			},
			want: &S3{
				Bucket:     testBucket,
				PathPrefix: "",
			},
			wantNilSessions: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewS3(tt.args.bucket, tt.args.pathPrefix, tt.args.conn, tt.args.session)

			if got.Bucket != tt.want.Bucket {
				t.Errorf("NewS3() Bucket = %v, want %v", got, tt.want)
				return
			}

			if got.PathPrefix != tt.want.PathPrefix {
				t.Errorf("NewS3() Bucket = %v, want %v", got, tt.want)
				return
			}

			if got.conn != nil && tt.wantNilSessions == true {
				t.Errorf("NewS3() conn = %v, want nil", got)
				return
			}

			if got.session != nil && tt.wantNilSessions == true {
				t.Errorf("NewS3() session = %v, want nil", got)
				return
			}
		})
	}
}

func TestS3_Read(t *testing.T) {
	type fields struct {
		Bucket     string
		PathPrefix string
		conn       *s3.S3
		session    *session.Session
	}
	type args struct {
		key string
	}
	type condition struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		condition condition
		want      []byte
		wantErr   bool
	}{
		{
			name: "object found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key: "test.txt",
			},
			condition: condition{
				preCondition: func() {
					_, err := s3.New(awsSession).PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test.txt"),
						Body:   strings.NewReader("test"),
					})
					if err != nil {
						t.Errorf("PutObject() error = %v", err)
					}
				},
				postCondition: func() {
					_, err := s3.New(awsSession).DeleteObject(&s3.DeleteObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test.txt"),
					})
					if err != nil {
						t.Errorf("DeleteObject() error = %v", err)
					}
				},
			},
			want:    []byte("test"),
			wantErr: false,
		},
		{
			name: "multiple objects with object found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key: "test.txt",
			},
			condition: condition{
				preCondition: func() {
					_, err := s3.New(awsSession).PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test.txt"),
						Body:   strings.NewReader("test"),
					})
					if err != nil {
						t.Errorf("PutObject() error = %v", err)
					}
					_, err = s3.New(awsSession).PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test2.txt"),
						Body:   strings.NewReader("test2"),
					})
					if err != nil {
						t.Errorf("PutObject() error = %v", err)
					}
				},
				postCondition: func() {
					_, err := s3.New(awsSession).DeleteObject(&s3.DeleteObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test.txt"),
					})
					if err != nil {
						t.Errorf("DeleteObject() error = %v", err)
					}
					_, err = s3.New(awsSession).DeleteObject(&s3.DeleteObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test2.txt"),
					})
					if err != nil {
						t.Errorf("DeleteObject() error = %v", err)
					}
				},
			},
			want:    []byte("test"),
			wantErr: false,
		},
		{
			name: "object not found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key: "test.txt",
			},
			condition: condition{
				preCondition: func() {
				},
				postCondition: func() {
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "objects with object not found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key: "test.txt",
			},
			condition: condition{
				preCondition: func() {
					_, err := s3.New(awsSession).PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test1.txt"),
						Body:   strings.NewReader("test"),
					})
					if err != nil {
						t.Errorf("PutObject() error = %v", err)
					}
					_, err = s3.New(awsSession).PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test2.txt"),
						Body:   strings.NewReader("test2"),
					})
					if err != nil {
						t.Errorf("PutObject() error = %v", err)
					}
				},
				postCondition: func() {
					_, err := s3.New(awsSession).DeleteObject(&s3.DeleteObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test1.txt"),
					})
					if err != nil {
						t.Errorf("DeleteObject() error = %v", err)
					}
					_, err = s3.New(awsSession).DeleteObject(&s3.DeleteObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test2.txt"),
					})
					if err != nil {
						t.Errorf("DeleteObject() error = %v", err)
					}
				},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.condition.preCondition()
			defer tt.condition.postCondition()
			s3def := S3{
				Bucket:     tt.fields.Bucket,
				PathPrefix: tt.fields.PathPrefix,
				conn:       tt.fields.conn,
				session:    tt.fields.session,
			}
			got, err := s3def.Read(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("S3.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				bts, err := ioutil.ReadAll(got)
				if (err != nil) != tt.wantErr {
					t.Errorf("S3.Read() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(bts, tt.want) {
					t.Errorf("S3.Read() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestS3_Write(t *testing.T) {
	type fields struct {
		Bucket     string
		PathPrefix string
		conn       *s3.S3
		session    *session.Session
	}
	type args struct {
		key   string
		value io.Reader
	}
	type condition struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		condition condition
		wantErr   bool
	}{
		{
			name: "object found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key:   "test.txt",
				value: strings.NewReader("test"),
			},
			condition: condition{
				preCondition: func() {
					s3Session := s3.New(awsSession)

					_, err := s3Session.PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test.txt"),
						Body:   strings.NewReader("test"),
					})
					if err != nil {
						t.Errorf("PutObject() error = %v", err)
						t.FailNow()
						return
					}
				},
				postCondition: func() {
					s3Session := s3.New(awsSession)
					defer func() {
						_, err := s3Session.DeleteObject(&s3.DeleteObjectInput{
							Bucket: aws.String(testBucket),
							Key:    aws.String("test.txt"),
						})
						if err != nil {
							t.Errorf("DeleteObject() error = %v", err)
							t.FailNow()
						}
					}()
					getObject, err := s3Session.GetObject(&s3.GetObjectInput{})
					if err != nil {
						t.Errorf("GetObject() error = %v", err)
						t.FailNow()
						return
					}
					defer getObject.Body.Close()
					if getObject.Body != nil {
						bts, err := ioutil.ReadAll(getObject.Body)
						if err != nil {
							t.Errorf("ReadAll() error = %v", err)
							t.FailNow()
							return
						}
						if string(bts) != "test" {
							t.Errorf("ReadAll() error = %v", err)
							t.FailNow()
							return
						}
					}
				},
			},
			wantErr: false,
		},
		{
			name: "object not found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key:   "test.txt",
				value: strings.NewReader("test"),
			},
			condition: condition{
				preCondition: func() {
				},
				postCondition: func() {
					s3Session := s3.New(awsSession)
					defer func() {
						_, err := s3Session.DeleteObject(&s3.DeleteObjectInput{
							Bucket: aws.String(testBucket),
							Key:    aws.String("test.txt"),
						})
						if err != nil {
							t.Errorf("DeleteObject() error = %v", err)
							t.FailNow()
						}
					}()
					getObject, err := s3Session.GetObject(&s3.GetObjectInput{})
					if err != nil {
						t.Errorf("GetObject() error = %v", err)
						t.FailNow()
						return
					}
					defer getObject.Body.Close()
					if getObject.Body != nil {
						bts, err := ioutil.ReadAll(getObject.Body)
						if err != nil {
							t.Errorf("ReadAll() error = %v", err)
							t.FailNow()
							return
						}
						if string(bts) != "test" {
							t.Errorf("ReadAll() error = %v", err)
							t.FailNow()
							return
						}
					}
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3def := S3{
				Bucket:     tt.fields.Bucket,
				PathPrefix: tt.fields.PathPrefix,
				conn:       tt.fields.conn,
				session:    tt.fields.session,
			}
			if err := s3def.Write(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("S3.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestS3_Delete(t *testing.T) {
	type fields struct {
		Bucket     string
		PathPrefix string
		conn       *s3.S3
		session    *session.Session
	}
	type args struct {
		key string
	}
	type condition struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		condition condition
		wantErr   bool
	}{
		{
			name: "object found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key: "test.txt",
			},
			condition: condition{
				preCondition: func() {
					s3Session := s3.New(awsSession)

					_, err := s3Session.PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test.txt"),
						Body:   strings.NewReader("test"),
					})
					if err != nil {
						t.Errorf("TestS3_Delete DeleteObject() error = %v", err)
						t.FailNow()
						return
					}
				},
				postCondition: func() {
					s3Session := s3.New(awsSession)
					defer func() {
						_, err := s3Session.DeleteObject(&s3.DeleteObjectInput{
							Bucket: aws.String(testBucket),
							Key:    aws.String("test.txt"),
						})
						if err != nil {
							t.Errorf("TestS3_Delete DeleteObject() error = %v", err)
							t.FailNow()
						}
					}()

					// we expect an error to be returned because the object should not exist any more
					getObject, err := s3Session.GetObject(&s3.GetObjectInput{})
					if err == nil {
						t.Errorf("TestS3_Delete GetObject() error is not nil = %v", err)
						t.FailNow()
						return
					}
					defer getObject.Body.Close()
				},
			},
			wantErr: false,
		},
		{
			name: "object not found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key: "test.txt",
			},
			condition: condition{
				preCondition: func() {
				},
				postCondition: func() {
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3def := S3{
				Bucket:     tt.fields.Bucket,
				PathPrefix: tt.fields.PathPrefix,
				conn:       tt.fields.conn,
				session:    tt.fields.session,
			}
			if err := s3def.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("S3.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestS3_Exists(t *testing.T) {
	type fields struct {
		Bucket     string
		PathPrefix string
		conn       *s3.S3
		session    *session.Session
	}
	type args struct {
		key string
	}
	type condition struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		condition condition
		want      bool
		wantErr   bool
	}{
		{
			name: "object found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key: "test.txt",
			},
			condition: condition{
				preCondition: func() {
					s3Session := s3.New(awsSession)

					_, err := s3Session.PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test.txt"),
						Body:   strings.NewReader("test"),
					})
					if err != nil {
						t.Errorf("TestS3_Exists DeleteObject() error = %v", err)
						t.FailNow()
						return
					}
				},
				postCondition: func() {
					s3Session := s3.New(awsSession)
					defer func() {
						_, err := s3Session.DeleteObject(&s3.DeleteObjectInput{
							Bucket: aws.String(testBucket),
							Key:    aws.String("test.txt"),
						})
						if err != nil {
							t.Errorf("TestS3_Exists DeleteObject() error = %v", err)
							t.FailNow()
						}
					}()
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "object not found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			args: args{
				key: "test.txt",
			},
			condition: condition{
				preCondition: func() {
				},
				postCondition: func() {
				},
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.condition.preCondition()
			defer tt.condition.postCondition()
			s3def := S3{
				Bucket:     tt.fields.Bucket,
				PathPrefix: tt.fields.PathPrefix,
				conn:       tt.fields.conn,
				session:    tt.fields.session,
			}
			got, err := s3def.Exists(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("S3.Exists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("S3.Exists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestS3_List(t *testing.T) {
	type fields struct {
		Bucket     string
		PathPrefix string
		conn       *s3.S3
		session    *session.Session
	}
	type condition struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name      string
		fields    fields
		condition condition
		want      []string
		wantErr   bool
	}{
		{
			name: "object found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			condition: condition{
				preCondition: func() {
					s3Session := s3.New(awsSession)

					_, err := s3Session.PutObject(&s3.PutObjectInput{
						Bucket: aws.String(testBucket),
						Key:    aws.String("test.txt"),
						Body:   strings.NewReader("test"),
					})
					if err != nil {
						t.Errorf("TestS3_List DeleteObject() error = %v", err)
						t.FailNow()
						return
					}
				},
				postCondition: func() {
					s3Session := s3.New(awsSession)
					defer func() {
						_, err := s3Session.DeleteObject(&s3.DeleteObjectInput{
							Bucket: aws.String(testBucket),
							Key:    aws.String("test.txt"),
						})
						if err != nil {
							t.Errorf("TestS3_List DeleteObject() error = %v", err)
							t.FailNow()
						}
					}()
				},
			},
			want:    []string{"test.txt"},
			wantErr: false,
		},
		{
			name: "object not found",
			fields: fields{
				Bucket:     testBucket,
				PathPrefix: "",
				conn:       s3.New(awsSession),
				session:    awsSession,
			},
			condition: condition{
				preCondition: func() {
				},
				postCondition: func() {
				},
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.condition.preCondition()
			defer tt.condition.postCondition()
			s3def := S3{
				Bucket:     tt.fields.Bucket,
				PathPrefix: tt.fields.PathPrefix,
				conn:       tt.fields.conn,
				session:    tt.fields.session,
			}
			got, err := s3def.List()
			if (err != nil) != tt.wantErr {
				t.Errorf("S3.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("S3.List() = %v, want %v", got, tt.want)
			}
		})
	}
}

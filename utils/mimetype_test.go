package utils

import (
	"bytes"
	"io"
	"testing"
	"testing/fstest"
)

func TestMimeType(t *testing.T) {
	type args struct {
		input io.Reader
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test.txt",
			args: args{
				input: func() io.Reader {
					fs := fstest.MapFS{
						"hello.txt": {
							Data: []byte("hello, world"),
						},
					}
					file, err := fs.Open("hello.txt")
					if err != nil {
						t.Fatal(err)
					}
					defer file.Close()
					return file
				}(),
			},
			want:    "text/plain; charset=utf-8",
			wantErr: false,
		},
		{
			name: "empty string",
			args: args{
				input: bytes.NewBufferString(""),
			},
			want:    "text/plain",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MimeType(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("MimeType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MimeType() = %v, want %v", got, tt.want)
			}
		})
	}
}

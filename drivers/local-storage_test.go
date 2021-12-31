package drivers

import (
	"bytes"
	"io/fs"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func TestNewLocalStorage(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name string
		args args
		want *LocalStorage
	}{
		{
			name: "should return LocalStorage",
			args: args{
				path: "/tmp/test",
			},
			want: &LocalStorage{
				Path: "/tmp/test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewLocalStorage(tt.args.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewLocalStorage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocalStorage_fullPath(t *testing.T) {
	type fields struct {
		Path        string
		Permissions *int
	}
	type args struct {
		file string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "should return full path /tmp/test",
			fields: fields{
				Path:        "/tmp/test",
				Permissions: nil,
			},
			args: args{
				file: "test.txt",
			},
			want: "/tmp/test/test.txt",
		},
		{
			name: "should return full path /var/lib/test",
			fields: fields{
				Path:        "/var/lib/test",
				Permissions: nil,
			},
			args: args{
				file: "test.txt",
			},
			want: "/var/lib/test/test.txt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := LocalStorage{
				Path:        tt.fields.Path,
				Permissions: tt.fields.Permissions,
			}
			if got := d.fullPath(tt.args.file); got != tt.want {
				t.Errorf("LocalStorage.fullPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocalStorage_filePermissions(t *testing.T) {
	type fields struct {
		Path        string
		Permissions *int
	}
	tests := []struct {
		name   string
		fields fields
		want   fs.FileMode
	}{
		{
			name: "should return 0644",
			fields: fields{
				Path:        "/tmp/test",
				Permissions: nil,
			},
			want: fs.FileMode(0644),
		},
		{
			name: "should return 0755",
			fields: fields{
				Path: "/tmp/test",
				Permissions: func() *int {
					i := int(0755)
					return &i
				}(),
			},
			want: fs.FileMode(0755),
		},
		{
			name: "should return 0555",
			fields: fields{
				Path: "/tmp/test",
				Permissions: func() *int {
					i := int(0555)
					return &i
				}(),
			},
			want: fs.FileMode(0555),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := LocalStorage{
				Path:        tt.fields.Path,
				Permissions: tt.fields.Permissions,
			}
			if got := d.filePermissions(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LocalStorage.filePermissions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocalStorage_Read(t *testing.T) {
	type fields struct {
		Path        string
		Permissions *int
	}
	type args struct {
		key string
	}
	type conditions struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		cond    conditions
		want    []byte
		wantErr bool
	}{
		{
			name: "return file values (file not empty)",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte("test"), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    []byte("test"),
			wantErr: false,
		},
		{
			name: "return file values (file empty)",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte(""), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    []byte(""),
			wantErr: false,
		},
		{
			name: "file not found",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "directory not exists",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition:  func() {},
				postCondition: func() {},
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cond.preCondition()
			defer tt.cond.postCondition()
			d := LocalStorage{
				Path:        tt.fields.Path,
				Permissions: tt.fields.Permissions,
			}
			got, err := d.Read(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("LocalStorage.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// we might have hit an error, so we don't want to compare the values
			// instead skip further checks
			if got == nil {
				return
			}

			bts, err := ioutil.ReadAll(got)
			if err != nil {
				t.Errorf("LocalStorage.Read() error reading file: %v", err)
				return
			}
			if !reflect.DeepEqual(bts, tt.want) {
				t.Errorf("LocalStorage.Read() = %v, want %v", got, tt.want)
				return
			}
		})
	}
}

func TestLocalStorage_Write(t *testing.T) {
	type fields struct {
		Path        string
		Permissions *int
	}
	type args struct {
		key   string
		value []byte
	}
	type conditions struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		cond            conditions
		expectFileValue []byte
		wantErr         bool
	}{
		{
			name: "write file not exists",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key:   "test.txt",
				value: []byte("test123"),
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			expectFileValue: []byte("test123"),
			wantErr:         false,
		},
		{
			name: "overwrite empty file",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key:   "test.txt",
				value: []byte("test123"),
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte(""), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			expectFileValue: []byte("test123"),
			wantErr:         false,
		},
		{
			name: "overwrite file that is not empty",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key:   "test.txt",
				value: []byte("test123"),
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte("test"), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			expectFileValue: []byte("test123"),
			wantErr:         false,
		},
		{
			name: "directory not exists",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition:  func() {},
				postCondition: func() {},
			},
			expectFileValue: nil,
			wantErr:         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cond.preCondition()
			defer tt.cond.postCondition()
			d := LocalStorage{
				Path:        tt.fields.Path,
				Permissions: tt.fields.Permissions,
			}
			if err := d.Write(tt.args.key, bytes.NewBuffer(tt.args.value)); (err != nil) != tt.wantErr {
				t.Errorf("LocalStorage.Write() error = %v, wantErr %v", err, tt.wantErr)
			}

			// we only the value if we do not expect an error
			if tt.wantErr == false {
				bts, err := ioutil.ReadFile(d.fullPath(tt.args.key))
				if err != nil {
					t.Errorf("TestLocalStorage_Write() error reading file: %v", err)
				}

				if !reflect.DeepEqual(bts, tt.expectFileValue) {
					t.Errorf("LocalStorage.Write() read = %v, want %v", bts, tt.expectFileValue)
				}
			}
		})
	}
}

func TestLocalStorage_Delete(t *testing.T) {
	type fields struct {
		Path        string
		Permissions *int
	}
	type args struct {
		key string
	}
	type conditions struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		cond    conditions
		wantErr bool
	}{
		{
			name: "file not exists",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			wantErr: true,
		},
		{
			name: "empty file",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte(""), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			wantErr: false,
		},
		{
			name: "file not empty",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte("test"), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			wantErr: false,
		},
		{
			name: "directory not exists",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition:  func() {},
				postCondition: func() {},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cond.preCondition()
			defer tt.cond.postCondition()
			d := LocalStorage{
				Path:        tt.fields.Path,
				Permissions: tt.fields.Permissions,
			}
			if err := d.Delete(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("LocalStorage.Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLocalStorage_Exists(t *testing.T) {
	type fields struct {
		Path        string
		Permissions *int
	}
	type args struct {
		key string
	}
	type conditions struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		cond    conditions
		want    bool
		wantErr bool
	}{
		{
			name: "file not exists",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    false,
			wantErr: true,
		},
		{
			name: "empty file",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte(""), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "file not empty",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte("test"), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    true,
			wantErr: false,
		},
		{
			name: "directory not exists",
			fields: fields{
				Path: "/tmp/test",
			},
			args: args{
				key: "test.txt",
			},
			cond: conditions{
				preCondition:  func() {},
				postCondition: func() {},
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cond.preCondition()
			defer tt.cond.postCondition()
			d := LocalStorage{
				Path:        tt.fields.Path,
				Permissions: tt.fields.Permissions,
			}
			got, err := d.Exists(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("LocalStorage.Exists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LocalStorage.Exists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocalStorage_List(t *testing.T) {
	type fields struct {
		Path        string
		Permissions *int
	}
	type conditions struct {
		preCondition  func()
		postCondition func()
	}
	tests := []struct {
		name    string
		fields  fields
		cond    conditions
		want    []string
		wantErr bool
	}{
		{
			name: "file not exists",
			fields: fields{
				Path: "/tmp/test",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    []string{},
			wantErr: false,
		},
		{
			name: "empty file",
			fields: fields{
				Path: "/tmp/test",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte(""), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    []string{"test.txt"},
			wantErr: false,
		},
		{
			name: "file not empty",
			fields: fields{
				Path: "/tmp/test",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test.txt", []byte("test"), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    []string{"test.txt"},
			wantErr: false,
		},
		{
			name: "multiple files",
			fields: fields{
				Path: "/tmp/test",
			},
			cond: conditions{
				preCondition: func() {
					err := os.MkdirAll("/tmp/test", 0755)
					if err != nil {
						t.Errorf("error creating directory: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test1.txt", []byte("test1"), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition 1: %v", err)
					}
					err = ioutil.WriteFile("/tmp/test/test2.txt", []byte("test2"), 0644)
					if err != nil {
						t.Errorf("TestLocalStorage_Read() preCondition 2: %v", err)
					}
				},
				postCondition: func() {
					err := os.RemoveAll("/tmp/test")
					if err != nil {
						t.Errorf("TestLocalStorage_Read() postCondition: %v", err)
					}
				},
			},
			want:    []string{"test1.txt", "test2.txt"},
			wantErr: false,
		},
		{
			name: "directory not exists",
			fields: fields{
				Path: "/tmp/test2",
			},
			cond: conditions{
				preCondition:  func() {},
				postCondition: func() {},
			},
			want:    []string{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.cond.preCondition()
			defer tt.cond.postCondition()
			d := LocalStorage{
				Path:        tt.fields.Path,
				Permissions: tt.fields.Permissions,
			}
			got, err := d.List()
			if (err != nil) != tt.wantErr {
				t.Errorf("LocalStorage.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("LocalStorage.List() = %v, want %v", got, tt.want)
			}
		})
	}
}

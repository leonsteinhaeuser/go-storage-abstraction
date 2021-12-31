package drivers

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
)

type LocalStorage struct {
	// Path defines the root directory of the local storage.
	Path string
	// Permissions defines the file permissions for the files in the local storage.
	// If not specified, the default value 0644 is used.
	Permissions *int
}

// NewLocalStorage creates a new LocalStorage instance.
// The path defines the root directory of the local storage.
func NewLocalStorage(path string) *LocalStorage {
	return &LocalStorage{
		Path: path,
	}
}

// fullPath returns the full path of the file.
func (d LocalStorage) fullPath(file string) string {
	return path.Join(d.Path, file)
}

// filePermissions returns the file permissions for the files in the local storage.
// If not specified, the default value 0644 is used.
func (d LocalStorage) filePermissions() fs.FileMode {
	if d.Permissions != nil {
		return fs.FileMode(*d.Permissions)
	}
	return 0644
}

// Read returns the value of the file identified by key.
// If the file does not exist, an error is returned.
func (d LocalStorage) Read(key string) (io.Reader, error) {
	path := d.fullPath(key)
	bts, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, path)
	}
	return bytes.NewBuffer(bts), nil
}

func (d LocalStorage) Write(key string, value io.Reader) error {
	filePath := d.fullPath(key)
	bts, err := ioutil.ReadAll(value)
	if err != nil {
		return fmt.Errorf("%w: %s", err, filePath)
	}
	err = ioutil.WriteFile(filePath, bts, d.filePermissions())
	if err != nil {
		return fmt.Errorf("%w: %s", err, filePath)
	}
	return nil
}

func (d LocalStorage) Delete(key string) error {
	path := d.fullPath(key)
	err := os.Remove(path)
	if err != nil {
		return fmt.Errorf("%w: %s", err, path)
	}
	return nil
}

func (d LocalStorage) Exists(key string) (bool, error) {
	path := d.fullPath(key)
	fInfo, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("%w: %s", err, path)
	}
	if fInfo.IsDir() {
		return false, nil
	}
	return true, nil
}

func (d LocalStorage) List() ([]string, error) {
	files, err := ioutil.ReadDir(d.Path)
	if err != nil {
		return []string{}, fmt.Errorf("%w: %s", err, d.Path)
	}
	fileName := []string{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileName = append(fileName, file.Name())
	}
	return fileName, nil
}

package gostorage

import "io"

// Driver is the interface that must be implemented by a storage driver.
// It describes the capabilities of a storage driver.
type Driver interface {
	// Read reads the file/object and returns the content.
	Read(key string) (io.Reader, error)
	// Write writes the content to the file/object.
	Write(key string, value io.Reader) error
	// Delete deletes the file/object.
	Delete(key string) error
	// Exists checks if the file/object exists.
	Exists(key string) (bool, error)
	// List lists all the files/objects.
	List() ([]string, error)
}

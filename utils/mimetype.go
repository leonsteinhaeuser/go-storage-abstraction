package utils

import (
	"fmt"
	"io"

	"github.com/gabriel-vasile/mimetype"
)

// MimeType returns the MIME type of the input reader.
func MimeType(input io.Reader) (string, error) {
	mType, err := mimetype.DetectReader(input)
	if err != nil {
		return "", fmt.Errorf("unable to detect MimeType: %w", err)
	}
	return mType.String(), nil
}

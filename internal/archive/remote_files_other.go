//go:build !darwin && !linux

package archive

import (
	"errors"
	"os"
)

func openRemoteResource(path string) (*os.File, error) { return os.Open(path) }

func readPrivateRemoteFile(string) ([]byte, error) {
	return nil, errors.New("private-files-unsupported")
}
func writePrivateRemoteFile(string, []byte) error { return errors.New("private-files-unsupported") }
func remoteArchiveFileIdentity(string) (string, error) {
	return "", errors.New("private-files-unsupported")
}

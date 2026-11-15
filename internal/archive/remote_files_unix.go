//go:build darwin || linux

package archive

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

func openRemoteResource(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
}

func remoteArchiveFileIdentity(path string) (string, error) {
	var stat unix.Stat_t
	if unix.Stat(path, &stat) != nil {
		return "", errors.New("archive-unavailable")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", errors.New("archive-unavailable")
	}
	return stableID("remote_archive", absolute, fmt.Sprint(stat.Dev), fmt.Sprint(stat.Ino)), nil
}

type remoteDirectoryNode struct {
	file *os.File
	name string
	stat unix.Stat_t
}

type remotePrivateDirectory struct {
	nodes []remoteDirectoryNode
}

func (d *remotePrivateDirectory) Fd() uintptr { return d.nodes[len(d.nodes)-1].file.Fd() }
func (d *remotePrivateDirectory) Sync() error { return d.nodes[len(d.nodes)-1].file.Sync() }
func (d *remotePrivateDirectory) Close() error {
	var err error
	for i := len(d.nodes) - 1; i >= 0; i-- {
		err = errors.Join(err, d.nodes[i].file.Close())
	}
	return err
}

func remoteTrustedDirectory(fd int, private bool) (unix.Stat_t, error) {
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR || (stat.Uid != 0 && stat.Uid != uint32(os.Geteuid())) || stat.Mode&0022 != 0 || !remoteAncestorACL(fd) {
		return stat, errors.New("unsafe-private-file")
	}
	if private && (stat.Uid != uint32(os.Geteuid()) || stat.Mode&0777 != 0700 || !remotePrivateACL(fd)) {
		return stat, errors.New("unsafe-private-file")
	}
	return stat, nil
}

func (d *remotePrivateDirectory) validate() error {
	for i, node := range d.nodes {
		stat, err := remoteTrustedDirectory(int(node.file.Fd()), i == len(d.nodes)-1)
		if err != nil || stat.Dev != node.stat.Dev || stat.Ino != node.stat.Ino || stat.Mode != node.stat.Mode || stat.Uid != node.stat.Uid || stat.Gid != node.stat.Gid {
			return errors.New("unsafe-private-file")
		}
		if i > 0 {
			var linked unix.Stat_t
			if unix.Fstatat(int(d.nodes[i-1].file.Fd()), node.name, &linked, unix.AT_SYMLINK_NOFOLLOW) != nil || linked.Dev != stat.Dev || linked.Ino != stat.Ino || linked.Mode != stat.Mode || linked.Uid != stat.Uid || linked.Gid != stat.Gid {
				return errors.New("unsafe-private-file")
			}
		}
	}
	return nil
}

func privateRemoteParent(path string) (*remotePrivateDirectory, string, error) {
	invalid := errors.New("unsafe-private-file")
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, "", invalid
	}
	parent, name := filepath.Dir(absolute), filepath.Base(absolute)
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, "", invalid
	}
	d := &remotePrivateDirectory{}
	root := os.NewFile(uintptr(fd), "/")
	stat, err := remoteTrustedDirectory(fd, false)
	if err != nil {
		root.Close()
		return nil, "", invalid
	}
	d.nodes = append(d.nodes, remoteDirectoryNode{file: root, stat: stat})
	for _, part := range strings.Split(strings.TrimPrefix(parent, "/"), "/") {
		if part == "" {
			continue
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if openErr != nil {
			d.Close()
			return nil, "", invalid
		}
		file := os.NewFile(uintptr(next), part)
		stat, err := remoteTrustedDirectory(next, false)
		if err != nil {
			file.Close()
			d.Close()
			return nil, "", invalid
		}
		d.nodes = append(d.nodes, remoteDirectoryNode{file: file, name: part, stat: stat})
		fd = next
	}
	if d.validate() != nil {
		d.Close()
		return nil, "", invalid
	}
	return d, name, nil
}

func readPrivateRemoteFile(path string) ([]byte, error) {
	invalid := errors.New("unsafe-private-file")
	parent, name, err := privateRemoteParent(path)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, invalid
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	var stat unix.Stat_t
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > remotePlanLimit || unix.Fstat(fd, &stat) != nil || stat.Uid != uint32(os.Geteuid()) || !remotePrivateACL(fd) {
		return nil, invalid
	}
	return readRemotePrivateContents(parent, name, f, stat, f)
}

func readRemotePrivateContents(parent *remotePrivateDirectory, name string, file *os.File, stat unix.Stat_t, reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, remotePlanLimit+1))
	if err != nil || len(data) > remotePlanLimit {
		return nil, errors.New("private-input-oversize")
	}
	var linked unix.Stat_t
	if parent.validate() != nil || unix.Fstatat(int(parent.Fd()), name, &linked, unix.AT_SYMLINK_NOFOLLOW) != nil || linked.Dev != stat.Dev || linked.Ino != stat.Ino || linked.Mode != stat.Mode || linked.Uid != stat.Uid || linked.Gid != stat.Gid || linked.Size != stat.Size || int64(len(data)) != stat.Size || !remotePrivateACL(int(file.Fd())) {
		return nil, errors.New("unsafe-private-file")
	}
	return data, nil
}

func writePrivateRemoteFile(path string, data []byte) error {
	invalid := errors.New("private-plan-write-failed")
	if len(data) > remotePlanLimit {
		return errors.New("plan-oversize")
	}
	parent, name, err := privateRemoteParent(path)
	if err != nil {
		return err
	}
	defer parent.Close()
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return invalid
	}
	temp := ".photoscrawl-plan-" + hex.EncodeToString(random[:])
	pfd := int(parent.Fd())
	fd, err := unix.Openat(pfd, temp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return invalid
	}
	defer unix.Unlinkat(pfd, temp, 0)
	f := os.NewFile(uintptr(fd), temp)
	defer f.Close()
	if f.Chmod(0600) != nil || !remotePrivateACL(fd) {
		return invalid
	}
	if _, err = f.Write(data); err != nil {
		return invalid
	}
	if f.Sync() != nil {
		return invalid
	}
	var published unix.Stat_t
	if parent.validate() != nil || unix.Fstat(fd, &published) != nil || published.Uid != uint32(os.Geteuid()) || published.Mode&0777 != 0600 || !remotePrivateACL(fd) {
		return invalid
	}
	if unix.Linkat(pfd, temp, pfd, name, 0) != nil {
		return errors.New("plan-publish-failed")
	}
	if parent.Sync() != nil {
		return invalid
	}
	return nil
}

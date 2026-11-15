package archive

import "golang.org/x/sys/unix"

func remotePrivateACL(fd int) bool {
	n, err := unix.Fgetxattr(fd, "system.posix_acl_access", nil)
	if err != unix.ENODATA && !(err == nil && n == 0) {
		return false
	}
	n, err = unix.Fgetxattr(fd, "system.posix_acl_default", nil)
	return err == unix.ENODATA || err == nil && n == 0
}

func remoteAncestorACL(fd int) bool { return remotePrivateACL(fd) }

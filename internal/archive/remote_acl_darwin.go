//go:build darwin && cgo

package archive

// #include "remote_acl_darwin.h"
import "C"

func remotePrivateACL(fd int) bool { return C.photoscrawl_private_acl(C.int(fd)) != 0 }

func remoteAncestorACL(fd int) bool { return C.photoscrawl_ancestor_acl(C.int(fd)) != 0 }

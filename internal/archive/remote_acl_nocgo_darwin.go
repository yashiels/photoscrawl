//go:build darwin && !cgo

package archive

func remotePrivateACL(int) bool { return false }

func remoteAncestorACL(int) bool { return false }

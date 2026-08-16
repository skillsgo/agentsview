//go:build !darwin || !cgo

package sync

func newWatchBackend(excludes []string) (watchBackend, error) {
	return newFSNotifyBackend(excludes)
}

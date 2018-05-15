// +build !windows

package file

import (
	"os"
	"syscall"
)

// while we're on unix, we may be running in a docker container that is pointed
// at a windows volume over samba. that doesn't support fsyncs on directories.
// try to detect this during init, and if so, don't bother with SyncDir.
var skipSyncDir bool

func init() {
	if pe, ok := SyncDir("/").(*os.PathError); ok {
		skipSyncDir = pe.Err == syscall.EINVAL
	}
}

func SyncDir(dirName string) error {
	if skipSyncDir {
		return nil
	}

	// fsync the dir to flush the rename
	dir, err := os.OpenFile(dirName, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// RenameFile will rename the source to target using os function.
func RenameFile(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

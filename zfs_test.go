package zfs_test

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
	zfs "github.com/edillmann/go-zfs"
	"strconv"
)

var handle *zfs.ZfsH

func sleep(delay int) {
	time.Sleep(time.Duration(delay) * time.Second)
}

func pow2(x int) int64 {
	return int64(math.Pow(2, float64(x)))
}

func getTestHandle() *zfs.ZfsH {
	return zfs.NewLocalHandle()
}

func getSSHTestHandle() *zfs.ZfsH {
	if (handle == nil) {
		handle = zfs.NewSSHHandle("localhost", 22, "root", nil)
		defer handle.Close()
	}
	return handle
}


//https://github.com/benbjohnson/testing
// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}

func zpoolTest(zh *zfs.ZfsH, t *testing.T, fn func()) {
	tempfiles := make([]string, 3)
	for i := range tempfiles {
		f, _ := ioutil.TempFile("/tmp/", "zfs-")
		defer f.Close()
		err := f.Truncate(pow2(30))
		ok(t, err)
		tempfiles[i] = f.Name()
		defer os.Remove(f.Name())
	}

	pool, err := zh.CreateZpool("test", nil, tempfiles...)
	ok(t, err)
	defer zh.DestroyZpool(pool)
	ok(t, err)
	fn()
}

/*
func TestDatasets1(t *testing.T) {
	zh := getSSHTestHandle()

	_, err := zh.Datasets("")
	ok(t, err)
}
*/

func TestDatasets(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {

		_, err := zh.Datasets("",99)
		ok(t, err)

		ds, err := zh.GetDataset("test")
		ok(t, err)
		equals(t, zfs.DatasetFilesystem, ds.Type)
		equals(t, "", ds.Origin)
		if runtime.GOOS != "solaris" {
			lused, err := strconv.ParseUint(ds.Logicalused, 10, 64)
			ok(t, err)
			assert(t, lused != 0, "Logicalused is not"+
				"greater than 0")
		}
	})
}

func TestSnapshots(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {
		snapshots, err := zh.SnapshotsByName("",99)
		ok(t, err)

		for _, snapshot := range snapshots {
			equals(t, zfs.DatasetSnapshot, snapshot.Type)
		}
	})
}

func TestFilesystems(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {
		f, err := zh.CreateFilesystem("test/filesystem-test", nil)
		ok(t, err)

		filesystems, err := zh.Filesystems("", 99)
		ok(t, err)

		for _, filesystem := range filesystems {
			equals(t, zfs.DatasetFilesystem, filesystem.Type)
		}

		ok(t, zh.Destroy(f, zfs.DestroyDefault))
	})
}

func TestCreateFilesystemWithProperties(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {
		props := map[string]string{
			"compression": "lz4",
		}

		f, err := zh.CreateFilesystem("test/filesystem-test", props)
		ok(t, err)

		equals(t, "lz4", f.Compression)

		filesystems, err := zh.Filesystems("", 99)
		ok(t, err)

		for _, filesystem := range filesystems {
			equals(t, zfs.DatasetFilesystem, filesystem.Type)
		}

		ok(t, zh.Destroy(f, zfs.DestroyDefault))
	})
}

func TestVolumes(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {

		v, err := zh.CreateVolume("test/volume-test", uint64(pow2(23)), nil)
		ok(t, err)

		// volumes are sometimes "busy" if you try to manipulate them right away
		sleep(1)

		equals(t, zfs.DatasetVolume, v.Type)
		volumes, err := zh.Volumes("", 99)
		ok(t, err)

		for _, volume := range volumes {
			equals(t, zfs.DatasetVolume, volume.Type)
		}

		ok(t, zh.Destroy(v, zfs.DestroyDefault))
	})
}
func TestSnapshot(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {

		f, err := zh.CreateFilesystem("test/snapshot-test", nil)
		ok(t, err)

		filesystems, err := zh.Filesystems("", 99)
		ok(t, err)

		for _, filesystem := range filesystems {
			equals(t, zfs.DatasetFilesystem, filesystem.Type)
		}

		s, err := zh.Snapshot(f, "test", false)
		ok(t, err)

		equals(t, zfs.DatasetSnapshot, s.Type)

		equals(t, "test/snapshot-test@test", s.Name)

		ok(t, zh.Destroy(s, zfs.DestroyDefault))

		ok(t, zh.Destroy(f,zfs.DestroyDefault))
	})
}

func TestClone(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {

		f, err := zh.CreateFilesystem("test/snapshot-test", nil)
		ok(t, err)

		filesystems, err := zh.Filesystems("", 99)
		ok(t, err)

		for _, filesystem := range filesystems {
			equals(t, zfs.DatasetFilesystem, filesystem.Type)
		}

		s, err := zh.Snapshot(f, "test", false)
		ok(t, err)

		equals(t, zfs.DatasetSnapshot, s.Type)
		equals(t, "test/snapshot-test@test", s.Name)

		c, err := zh.Clone(s, "test/clone-test", nil)
		ok(t, err)

		equals(t, zfs.DatasetFilesystem, c.Type)

		ok(t, zh.Destroy(c, zfs.DestroyDefault))

		ok(t, zh.Destroy(s, zfs.DestroyDefault))

		ok(t, zh.Destroy(f, zfs.DestroyDefault))
	})
}

func TestSendSnapshot(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {
		f, err := zh.CreateFilesystem("test/snapshot-test", nil)
		ok(t, err)

		filesystems, err := zh.Filesystems("", 99)
		ok(t, err)

		for _, filesystem := range filesystems {
			equals(t, zfs.DatasetFilesystem, filesystem.Type)
		}

		s, err := zh.Snapshot(f, "test", false)
		ok(t, err)

		file, _ := ioutil.TempFile("/tmp/", "zfs-")
		defer file.Close()
		err = file.Truncate(pow2(30))
		ok(t, err)
		defer os.Remove(file.Name())

		err = zh.SendSnapshot(s.Name, "", file, zfs.SendDefault, "")
		ok(t, err)

		ok(t, zh.Destroy(s, zfs.DestroyDefault))

		ok(t, zh.Destroy(f, zfs.DestroyDefault))
	})
}

func TestChildren(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {
		f, err := zh.CreateFilesystem("test/snapshot-test", nil)
		ok(t, err)

		s, err := zh.Snapshot(f, "test", false)
		ok(t, err)

		equals(t, zfs.DatasetSnapshot, s.Type)
		equals(t, "test/snapshot-test@test", s.Name)

		children, err := zh.Children(f, 0)
		ok(t, err)

		equals(t, 1, len(children))
		equals(t, "test/snapshot-test@test", children[0].Name)

		ok(t, zh.Destroy(s, zfs.DestroyDefault))
		ok(t, zh.Destroy(f, zfs.DestroyDefault))
	})
}

func TestListZpool(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {

		pools, err := zh.ListZpools()
		ok(t, err)
		var i int
		var pool *zfs.Zpool
		for i, pool = range pools {
			if pool.Name == "test" {
				break
			}
		}
		equals(t, "test", pools[i].Name)
	})
}

func TestRollback(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {

		f, err := zh.CreateFilesystem("test/snapshot-test", nil)
		ok(t, err)

		filesystems, err := zh.Filesystems("", 99)
		ok(t, err)

		for _, filesystem := range filesystems {
			equals(t, zfs.DatasetFilesystem, filesystem.Type)
		}

		s1, err := zh.Snapshot(f, "test", false)
		ok(t, err)

		_, err = zh.Snapshot(f, "test2", false)
		ok(t, err)

		s3, err := zh.Snapshot(f, "test3", false)
		ok(t, err)

		err = zh.Rollback(s3, false)
		ok(t, err)

		err = zh.Rollback(s1,false)
		assert(t, err != nil, "should error when rolling back beyond most recent without destroyMoreRecent = true")

		err = zh.Rollback(s1, true)
		ok(t, err)

		ok(t, zh.Destroy(s1,zfs.DestroyDefault))

		ok(t, zh.Destroy(f,zfs.DestroyDefault))
	})
}

func TestDiff(t *testing.T) {
	zh := getSSHTestHandle()
	zpoolTest(zh, t, func() {

		fs, err := zh.CreateFilesystem("test/origin", nil)
		ok(t, err)

		linkedFile, err := os.Create(filepath.Join(fs.Mountpoint, "linked"))
		ok(t, err)

		movedFile, err := os.Create(filepath.Join(fs.Mountpoint, "file"))
		ok(t, err)

		snapshot, err := zh.Snapshot(fs,"snapshot", false)
		ok(t, err)

		unicodeFile, err := os.Create(filepath.Join(fs.Mountpoint, "i ❤ unicode"))
		ok(t, err)

		err = os.Rename(movedFile.Name(), movedFile.Name()+"-new")
		ok(t, err)

		err = os.Link(linkedFile.Name(), linkedFile.Name()+"_hard")
		ok(t, err)

		inodeChanges, err := zh.Diff(fs,snapshot.Name)
		ok(t, err)
		equals(t, 4, len(inodeChanges))

		equals(t, "/test/origin/", inodeChanges[0].Path)
		equals(t, zfs.Directory, inodeChanges[0].Type)
		equals(t, zfs.Modified, inodeChanges[0].Change)

		equals(t, "/test/origin/linked", inodeChanges[1].Path)
		equals(t, zfs.File, inodeChanges[1].Type)
		equals(t, zfs.Modified, inodeChanges[1].Change)
		equals(t, 1, inodeChanges[1].ReferenceCountChange)

		equals(t, "/test/origin/file", inodeChanges[2].Path)
		equals(t, "/test/origin/file-new", inodeChanges[2].NewPath)
		equals(t, zfs.File, inodeChanges[2].Type)
		equals(t, zfs.Renamed, inodeChanges[2].Change)

		equals(t, "/test/origin/i ❤ unicode", inodeChanges[3].Path)
		equals(t, zfs.File, inodeChanges[3].Type)
		equals(t, zfs.Created, inodeChanges[3].Change)

		ok(t, movedFile.Close())
		ok(t, unicodeFile.Close())
		ok(t, linkedFile.Close())
		ok(t, zh.Destroy(snapshot,zfs.DestroyForceUmount))
		ok(t, zh.Destroy(fs,zfs.DestroyForceUmount))
	})
}

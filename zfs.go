// Package zfs provides wrappers around the ZFS command line tools.
package zfs

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"regexp"
	"golang.org/x/crypto/ssh"
	"os/user"
)

// ZFS dataset types, which can indicate if a dataset is a filesystem,
// snapshot, or volume.
const (
	DatasetFilesystem = "filesystem"
	DatasetSnapshot   = "snapshot"
	DatasetVolume     = "volume"
	DatasetBookmark   = "bookmark"
	DatasetAll        = "all"
)

// Dataset is a ZFS dataset.  A dataset could be a clone, filesystem, snapshot, bookmark
// or volume.  The Type struct member can be used to determine a dataset's type.
//
// The field definitions can be found in the ZFS manual:
// http://www.freebsd.org/cgi/man.cgi?zfs(8).
type Dataset struct {
	Name               string
	Origin             string
	Used               string
	Avail              string
	Mountpoint         string
	Compression        string
	Type               string
	Written            string
	Volsize            string
	Logicalused        string
	Quota              string
	ReceiveResumeToken string
	Compressratio      string
	Usedbysnapshots    string
}


// InodeType is the type of inode as reported by Diff
type InodeType int

// Types of Inodes
const (
	_                     = iota // 0 == unknown type
	BlockDevice InodeType = iota
	CharacterDevice
	Directory
	Door
	NamedPipe
	SymbolicLink
	EventPort
	Socket
	File
)

// ChangeType is the type of inode change as reported by Diff
type ChangeType int

// Types of Changes
const (
	_                  = iota // 0 == unknown type
	Removed ChangeType = iota
	Created
	Modified
	Renamed
)

// DestroyFlag is the options flag passed to Destroy
type DestroyFlag int

// Valid destroy options
const (
	DestroyDefault         DestroyFlag = 1 << iota
	DestroyRecursive                   = 1 << iota
	DestroyRecursiveClones             = 1 << iota
	DestroyDeferDeletion               = 1 << iota
	DestroyForceUmount                 = 1 << iota
)

type SendFlag int

const (
	SendDefault	       SendFlag = 1 << iota
	SendIncremental 	= 1 << iota
	SendRecursive 		= 1 << iota
	SendIntermediate 	= 1 << iota
	SendLz4		 		= 1 << iota
	SendEmbeddedData	= 1 << iota
	SendWithToken 		= 1 << iota
)

// InodeChange represents a change as reported by Diff
type InodeChange struct {
	Change               ChangeType
	Type                 InodeType
	Path                 string
	NewPath              string
	ReferenceCountChange int
}

// Logger can be used to log commands/actions
type Logger interface {
	Log(cmd []string)
}

type defaultLogger struct{}

func (*defaultLogger) Log(cmd []string) {
	return
}

var logger Logger = &defaultLogger{}

// SetLogger set a log handler to log all commands including arguments before
// they are executed
func SetLogger(l Logger) {
	if l != nil {
		logger = l
	}
}

// zfs handle used to redirect command
// to local or remote host over ssh
type ZfsH struct {
	Local    bool
	host     string
	port     int
	username string
	password string
	keyfile  string
	lz4Send  bool
	client   *ssh.Client
}

func (z *ZfsH) Lz4Send() bool {
	return z.lz4Send
}

func NewLocalHandle() *ZfsH {
	return &ZfsH{
		Local:true,
	}
}

func NewSSHHandle(host string, port int, username string, keyfile *string) *ZfsH {
	zh := &ZfsH{
		Local:false,
		host: host,
		port: port,
		username: username,
	}

	if (keyfile == nil) {
		usr, _ := user.Current()
		zh.keyfile = usr.HomeDir + "/.ssh/id_dsa"
	} else {
		zh.keyfile = *keyfile
	}

	return zh;
}

func (d *Dataset) DataSetName() string {
	if d.Type == DatasetSnapshot {
		return strings.Split(d.Name, "@")[1]
	}
	if d.Type == DatasetBookmark {
		return strings.Split(d.Name, "#")[1]
	}
	return ""
}

func (z *ZfsH) TestLz4SendSupport() {
	_, err := z.zfs("send","--help")
	if err != nil {
		zerr := err.(*Error)
		match, _ := regexp.MatchString("send \\[-.*c.*] \\[-\\[i", zerr.Stderr)
		z.lz4Send = match
	}
}

func (z *ZfsH) Close() {
	if (z.client != nil) {
		z.client.Close()
	}
}

// zfs is a helper function to wrap typical calls to zfs.
func (z *ZfsH) zfs(arg ...string) ([][]string, error) {
	c := command{
		Command: "zfs",
		zh: z,
	}
	return c.Run(arg...)
}

// Datasets returns a slice of ZFS datasets, regardless of type.
// A filter argument may be passed to select a dataset with the matching name,
// or empty string ("") may be used to select all datasets.
func (z *ZfsH) Datasets(datasettype string, filter string, depth int, recurse bool) ([]*Dataset, error) {
	return z.listByType(datasettype, filter, depth, recurse)
}

// Snapshots returns a slice of ZFS snapshots.
// A filter argument may be passed to select a snapshot with the matching name,
// or empty string ("") may be used to select all snapshots.
func (z *ZfsH) SnapshotsByName(filter string, depth int) ([]*Dataset, error) {
	return z.listByType(DatasetSnapshot, filter, depth, true)
}

// Bookmarks returns a slice of ZFS bookmarks.
// A filter argument may be passed to select a bookmark with the matching name,
// or empty string ("") may be used to select all bookmarks.
func (z *ZfsH) BookmarksByName(filter string, depth int) ([]*Dataset, error) {
	return z.listByType(DatasetBookmark, filter, depth, true)
}

// Filesystems returns a slice of ZFS filesystems.
// A filter argument may be passed to select a filesystem with the matching name,
// or empty string ("") may be used to select all filesystems.
func (z *ZfsH) Filesystems(filter string, depth int) ([]*Dataset, error) {
	return z.listByType(DatasetFilesystem, filter, depth, false)
}

// Volumes returns a slice of ZFS volumes.
// A filter argument may be passed to select a volume with the matching name,
// or empty string ("") may be used to select all volumes.
func (z *ZfsH) Volumes(filter string, depth int) ([]*Dataset, error) {
	return z.listByType(DatasetVolume, filter, depth, false)
}

// GetDataset retrieves a single ZFS dataset by name.  This dataset could be
// any valid ZFS dataset type, such as a clone, filesystem, snapshot, bookmark or volume.
func (z *ZfsH) GetDataset(name string) (*Dataset, error) {
	out, err := z.zfs("list", "-Hp", "-o", strings.Join(DsPropList, ","), name)
	if err != nil {
		return nil, err
	}

	ds := &Dataset{Name: name}
	for _, line := range out {
		if err := ds.parseLine(line); err != nil {
			return nil, err
		}
	}

	return ds, nil
}

// Clone clones a ZFS snapshot and returns a clone dataset.
// An error will be returned if the input dataset is not of snapshot type.
func (z *ZfsH) Clone(d *Dataset,dest string, properties map[string]string) (*Dataset, error) {
	if d.Type != DatasetSnapshot {
		return nil, errors.New("can only clone snapshots")
	}
	args := make([]string, 2, 4)
	args[0] = "clone"
	args[1] = "-p"
	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}
	args = append(args, []string{d.Name, dest}...)
	_, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(dest)
}

// Unmount unmounts currently mounted ZFS file systems.
func (z *ZfsH) Unmount(d *Dataset, force bool) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, errors.New("cannot unmount snapshots")
	}
	args := make([]string, 1, 3)
	args[0] = "umount"
	if force {
		args = append(args, "-f")
	}
	args = append(args, d.Name)
	_, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(d.Name)
}

// Mount mounts ZFS file systems.
func (z *ZfsH) Mount(d *Dataset, overlay bool, options []string) (*Dataset, error) {
	if d.Type == DatasetSnapshot {
		return nil, errors.New("cannot mount snapshots")
	}
	args := make([]string, 1, 5)
	args[0] = "mount"
	if overlay {
		args = append(args, "-O")
	}
	if options != nil {
		args = append(args, "-o")
		args = append(args, strings.Join(options, ","))
	}
	args = append(args, d.Name)
	_, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(d.Name)
}

// Mount mounts ZFS file systems.
func (z *ZfsH) AbortReceive(name string) (*Dataset, error) {
	args := make([]string, 1, 5)
	args[0] = "receive"
	args = append(args, "-A")
	args = append(args, name)
	_, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(name)
}

// ReceiveSnapshot receives a ZFS stream from the input io.Reader, creates a
// new snapshot with the specified name, and streams the input data into the
// newly-created snapshot.
// name destination dataset name
// uncompress uncompress prog if != "" (ex. lzop -d)
func (z *ZfsH) ReceiveSnapshot(input io.Reader, name, uncompress string, props []string) (*Dataset, error) {

	c := command{
		Command: "zfs",
		Stdin: input,
		zh: z,
	}

	if uncompress != "" {
		c.Command = uncompress+"|zfs"
	}
	args := make([]string, 1,5)
	args[0] = "receive"
	// resumable receive
	for _,prop := range props {
		if strings.Contains(prop,"=") {
			args = append(args, "-o")
			args = append(args, prop)
		}
	}
	args = append(args, "-s")
	args = append(args, name)

	_, err := c.Run(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(name)
}

// SendSnapshot sends a ZFS stream of a snapshot to the input io.Writer.
// An error will be returned if the input dataset is not of snapshot type.
// ds0 source snapshot
// ds1 previous snapshot used when sendflags is SendIncremental
// compression prog to pipe through if != "" (ex. lzop)
func (z *ZfsH) SendSnapshot(ds0, ds1 string, output io.Writer, sendflags SendFlag, compress string) error {
	if sendflags&SendWithToken == 0 && !strings.ContainsAny(ds0, "@") {
		return errors.New("can only send snapshots")
	}

	c := command{
		Command: "zfs",
		Stdout: output,
		zh: z,
	}

	args := make([]string, 1,5)
	args[0] = "send"

	if sendflags&SendRecursive != 0 {
		args = append(args, "-R")
	}

	if sendflags&SendLz4 != 0 {
		args = append(args, "-c")
	}

	if sendflags&SendWithToken != 0 {
		args = append(args, "-t")
	}

	if sendflags&SendEmbeddedData != 0 {
		args = append(args, "-e")
	}

	if sendflags&SendIncremental != 0 {
		if ds1 == "" {
			return errors.New("Source snapshot must be set for incremental send")
		}
		if sendflags&SendIntermediate != 0 {
			args = append(args, "-I", ds1)
		} else {
			args = append(args, "-i", ds1)
		}
	}
	args = append(args, ds0)

	if compress != "" {
		args = append(args, "|", compress)
	}

	_, err := c.Run(args...)
	return err
}

// CreateVolume creates a new ZFS volume with the specified name, size, and
// properties.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (z *ZfsH) CreateVolume(name string, size uint64, properties map[string]string) (*Dataset, error) {
	args := make([]string, 4, 5)
	args[0] = "create"
	args[1] = "-p"
	args[2] = "-V"
	args[3] = strconv.FormatUint(size, 10)
	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}
	args = append(args, name)
	_, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(name)
}

// Destroy destroys a ZFS dataset. If the destroy bit flag is set, any
// descendents of the dataset will be recursively destroyed, including snapshots.
// If the deferred bit flag is set, the snapshot is marked for deferred
// deletion.
func (z *ZfsH) Destroy(d *Dataset, flags DestroyFlag) error {
	args := make([]string, 1, 3)
	args[0] = "destroy"
	if flags&DestroyRecursive != 0 {
		args = append(args, "-r")
	}

	if flags&DestroyRecursiveClones != 0 {
		args = append(args, "-R")
	}

	if flags&DestroyDeferDeletion != 0 {
		args = append(args, "-d")
	}

	if flags&DestroyForceUmount != 0 {
		args = append(args, "-f")
	}

	args = append(args, d.Name)
	_, err := z.zfs(args...)
	return err
}

// SetProperty sets a ZFS property on the receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (z *ZfsH) SetProperty(d *Dataset, key, val string) error {
	prop := strings.Join([]string{key, val}, "=")
	_, err := z.zfs("set", prop, d.Name)
	return err
}

// GetProperty returns the current value of a ZFS property from the
// receiving dataset.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (z *ZfsH) GetProperty(d *Dataset, key string) (string, error) {
	out, err := z.zfs("get","-Hp", key, d.Name)
	if err != nil {
		return "", err
	}

	return out[0][2], nil
}

// Rename renames a dataset.
func (z *ZfsH) Rename( d *Dataset, name string, createParent bool, recursiveRenameSnapshots bool) (*Dataset, error) {
	args := make([]string, 3, 5)
	args[0] = "rename"
	args[1] = d.Name
	args[2] = name
	if createParent {
		args = append(args, "-p")
	}
	if recursiveRenameSnapshots {
		args = append(args, "-r")
	}
	_, err := z.zfs(args...)
	if err != nil {
		return d, err
	}

	return z.GetDataset(name)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (z *ZfsH) Snapshots(d *Dataset, depth int) ([]*Dataset, error) {
	return z.SnapshotsByName(d.Name, depth)
}

// Snapshots returns a slice of all ZFS snapshots of a given dataset.
func (z *ZfsH) Bookmarks(d *Dataset, depth int) ([]*Dataset, error) {
	return z.BookmarksByName(d.Name, depth)
}

// CreateFilesystem creates a new ZFS filesystem with the specified name and
// properties.
// A full list of available ZFS properties may be found here:
// https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (z *ZfsH) CreateFilesystem(name string, properties map[string]string) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "create"

	if properties != nil {
		args = append(args, propsSlice(properties)...)
	}

	args = append(args, name)
	_, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(name)
}

// Snapshot creates a new ZFS snapshot of the receiving dataset, using the
// specified name.  Optionally, the snapshot can be taken recursively, creating
// snapshots of all descendent filesystems in a single, atomic operation.
func (z *ZfsH) Snapshot(d *Dataset, name string, recursive bool) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "snapshot"
	if recursive {
		args = append(args, "-r")
	}
	snapName := fmt.Sprintf("%s@%s", d.Name, name)
	args = append(args, snapName)
	_, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(snapName)
}

// Snapshot creates a new ZFS snapshot of the receiving dataset, using the
// specified name.  Optionally, the snapshot can be taken recursively, creating
// snapshots of all descendent filesystems in a single, atomic operation.
func (z *ZfsH) Bookmark(d *Dataset, name string, recursive bool) (*Dataset, error) {
	args := make([]string, 1, 4)
	args[0] = "bookmark"
	if recursive {
		args = append(args, "-r")
	}
	snapName := fmt.Sprintf("%s@%s", d.Name, name)
	bookMarkName := fmt.Sprintf("%s#%s", d.Name, name)
	args = append(args, snapName, bookMarkName)
	_, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	return z.GetDataset(snapName)
}

// Rollback rolls back the receiving ZFS dataset to a previous snapshot.
// Optionally, intermediate snapshots can be destroyed.  A ZFS snapshot
// rollback cannot be completed without this option, if more recent
// snapshots exist.
// An error will be returned if the input dataset is not of snapshot type.
func (z *ZfsH) Rollback(d *Dataset, destroyMoreRecent bool) error {
	if d.Type != DatasetSnapshot {
		return errors.New("can only rollback snapshots")
	}

	args := make([]string, 1, 3)
	args[0] = "rollback"
	if destroyMoreRecent {
		args = append(args, "-r")
	}
	args = append(args, d.Name)

	_, err := z.zfs(args...)
	return err
}

// Children returns a slice of children of the receiving ZFS dataset.
// A recursion depth may be specified, or a depth of 0 allows unlimited
// recursion.
func (z *ZfsH) Children(d *Dataset, depth uint64) ([]*Dataset, error) {
	args := []string{"list"}
	if depth > 0 {
		args = append(args, "-d")
		args = append(args, strconv.FormatUint(depth, 10))
	} else {
		args = append(args, "-r")
	}
	args = append(args, "-t", "all", "-Hp", "-o", strings.Join(DsPropList, ","))
	args = append(args, d.Name)

	out, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}

	var datasets []*Dataset
	name := ""
	var ds *Dataset
	for _, line := range out {
		if name != line[0] {
			name = line[0]
			ds = &Dataset{Name: name}
			datasets = append(datasets, ds)
		}
		if err := ds.parseLine(line); err != nil {
			return nil, err
		}
	}
	return datasets[1:], nil
}

// Diff returns changes between a snapshot and the given ZFS dataset.
// The snapshot name must include the filesystem part as it is possible to
// compare clones with their origin snapshots.
func (z *ZfsH) Diff(d *Dataset, snapshot string) ([]*InodeChange, error) {
	args := []string{"diff", "-FH", snapshot, d.Name}[:]
	out, err := z.zfs(args...)
	if err != nil {
		return nil, err
	}
	inodeChanges, err := parseInodeChanges(out)
	if err != nil {
		return nil, err
	}
	return inodeChanges, nil
}

package zfs

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"github.com/pborman/uuid"
	"bytes"
	"golang.org/x/crypto/ssh"
)

type command struct {
	zh *ZfsH
	Path string
	Env []string
	Command string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	stdout bytes.Buffer
	stderr bytes.Buffer
}

type waitable interface {
	Wait() error
}

func (cmd *command) LocalPrepare(arg ...string) (*exec.Cmd) {

	var lcmd *exec.Cmd

	if (strings.Contains(cmd.Command,"|")) {
		// simple command piping
		c := strings.Join(arg," ")
		lcmd = exec.Command("sh", "-c", cmd.Command+" "+c)
	} else {
		lcmd = exec.Command(cmd.Command, arg...)
	}

	if cmd.Stdout == nil {
		lcmd.Stdout = &cmd.stdout
	} else {
		lcmd.Stdout = cmd.Stdout
	}

	if cmd.Stdin != nil {
		lcmd.Stdin = cmd.Stdin

	}
	if cmd.Stderr == nil {
		lcmd.Stderr = &cmd.stderr
	} else {
		lcmd.Stderr = cmd.Stderr
	}
	return lcmd
}

func (c *command) Run(arg ...string) ([][]string, error) {

	var err error
	var cmd waitable
	var session *ssh.Session

	joinedArgs := strings.Join(arg, " ")
	c.Path = c.Command+" "+joinedArgs
	c.Env = []string{"LC_CTYPE=C", "LANG=en_US.UTF-8"}
	id := uuid.New()
	if (c.zh.Local) {
		logger.Log([]string{"LOCAL:" + id, "START", c.Path})
		lcmd := c.LocalPrepare(arg...)
		err = lcmd.Start()
		cmd = lcmd
	} else {
		logger.Log([]string{"REMOTE:" + id, "START", c.Path})
		err, session = c.StartCommand()
		if (session != nil) {
			defer func() {
				session.Close()
			}()
		}
		cmd = session
	}

	logger.Log([]string{"ID:" + id, "DONE"})

	if err != nil {
		return nil, &Error{
			Err:    err,
			Debug:  strings.Join([]string{c.Command, joinedArgs}, " "),
			Stderr: c.stderr.String(),
		}
	}

	if err = cmd.Wait(); err != nil {
		return nil, &Error{
			Err:    err,
			Stderr: c.stderr.String(),
			Debug:  strings.Join([]string{c.Command, joinedArgs}, " "),
		}
	}

	// assume if you passed in something for stdout, that you know what to do with it
	if c.Stdout != nil {
		return nil, nil
	}

	lines := strings.Split(c.stdout.String(), "\n")
	//last line is always blank
	lines = lines[0 : len(lines)-1]
	output := make([][]string, len(lines))

	for i, l := range lines {
		output[i] = strings.Fields(l)
	}
	return output, err

}

func setString(field *string, value string) {
	v := ""
	if value != "-" {
		v = value
	}
	*field = v
}

func setUint(field *uint64, value string) error {
	var v uint64
	if value != "-" {
		var err error
		v, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
	}
	*field = v
	return nil
}

func (ds *Dataset) parseLine(line []string) error {
	if len(line) != len(DsPropList) {
		return errors.New("ZFS output does not match what is expected" +
			"on this platform")
	}
	setString(&ds.Name, line[0])
	setString(&ds.Avail, line[3])
	setString(&ds.Compression, line[5])
	setString(&ds.Mountpoint, line[4])
	setString(&ds.Quota, line[8])
	setString(&ds.Type, line[6])
	setString(&ds.Origin, line[1])
	setString(&ds.Used, line[2])
	setString(&ds.Volsize, line[7])

	if runtime.GOOS != "solaris" {
		setString(&ds.Written, line[9])
		setString(&ds.Logicalused, line[10])
		setString(&ds.ReceiveResumeToken, line[11])
	}
	return nil
}

/*
 * from zfs diff`s escape function:
 *
 * Prints a file name out a character at a time.  If the character is
 * not in the range of what we consider "printable" ASCII, display it
 * as an escaped 3-digit octal value.  ASCII values less than a space
 * are all control characters and we declare the upper end as the
 * DELete character.  This also is the last 7-bit ASCII character.
 * We choose to treat all 8-bit ASCII as not printable for this
 * application.
 */
func unescapeFilepath(path string) (string, error) {
	buf := make([]byte, 0, len(path))
	llen := len(path)
	for i := 0; i < llen; {
		if path[i] == '\\' {
			if llen < i+5 {
				return "", fmt.Errorf("Invalid octal code: too short")
			}
			octalCode := path[(i + 1):(i + 5)]
			val, err := strconv.ParseUint(octalCode, 8, 8)
			if err != nil {
				return "", fmt.Errorf("Invalid octal code: %v", err)
			}
			buf = append(buf, byte(val))
			i += 5
		} else {
			buf = append(buf, path[i])
			i++
		}
	}
	return string(buf), nil
}

var changeTypeMap = map[string]ChangeType{
	"-": Removed,
	"+": Created,
	"M": Modified,
	"R": Renamed,
}
var inodeTypeMap = map[string]InodeType{
	"B": BlockDevice,
	"C": CharacterDevice,
	"/": Directory,
	">": Door,
	"|": NamedPipe,
	"@": SymbolicLink,
	"P": EventPort,
	"=": Socket,
	"F": File,
}

// matches (+1) or (-1)
var referenceCountRegex = regexp.MustCompile("\\(([+-]\\d+?)\\)")

func parseReferenceCount(field string) (int, error) {
	matches := referenceCountRegex.FindStringSubmatch(field)
	if matches == nil {
		return 0, fmt.Errorf("Regexp does not match")
	}
	return strconv.Atoi(matches[1])
}

func parseInodeChange(line []string) (*InodeChange, error) {
	llen := len(line)
	if llen < 1 {
		return nil, fmt.Errorf("Empty line passed")
	}

	changeType := changeTypeMap[line[0]]
	if changeType == 0 {
		return nil, fmt.Errorf("Unknown change type '%s'", line[0])
	}

	switch changeType {
	case Renamed:
		if llen != 4 {
			return nil, fmt.Errorf("Mismatching number of fields: expect 4, got: %d", llen)
		}
	case Modified:
		if llen != 4 && llen != 3 {
			return nil, fmt.Errorf("Mismatching number of fields: expect 3..4, got: %d", llen)
		}
	default:
		if llen != 3 {
			return nil, fmt.Errorf("Mismatching number of fields: expect 3, got: %d", llen)
		}
	}

	inodeType := inodeTypeMap[line[1]]
	if inodeType == 0 {
		return nil, fmt.Errorf("Unknown inode type '%s'", line[1])
	}

	path, err := unescapeFilepath(line[2])
	if err != nil {
		return nil, fmt.Errorf("Failed to parse filename: %v", err)
	}

	var newPath string
	var referenceCount int
	switch changeType {
	case Renamed:
		newPath, err = unescapeFilepath(line[3])
		if err != nil {
			return nil, fmt.Errorf("Failed to parse filename: %v", err)
		}
	case Modified:
		if llen == 4 {
			referenceCount, err = parseReferenceCount(line[3])
			if err != nil {
				return nil, fmt.Errorf("Failed to parse reference count: %v", err)
			}
		}
	default:
		newPath = ""
	}

	return &InodeChange{
		Change:               changeType,
		Type:                 inodeType,
		Path:                 path,
		NewPath:              newPath,
		ReferenceCountChange: referenceCount,
	}, nil
}

// example input
//M       /       /testpool/bar/
//+       F       /testpool/bar/hello.txt
//M       /       /testpool/bar/hello.txt (+1)
//M       /       /testpool/bar/hello-hardlink
func parseInodeChanges(lines [][]string) ([]*InodeChange, error) {
	changes := make([]*InodeChange, len(lines))

	for i, line := range lines {
		c, err := parseInodeChange(line)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse line %d of zfs diff: %v, got: '%s'", i, err, line)
		}
		changes[i] = c
	}
	return changes, nil
}

func (z *ZfsH) listByType(t, filter string, depth int, recurse bool) ([]*Dataset, error) {
	args := []string{"list", "-H", "-t", t, "-o", strings.Join(DsPropList, ",")}

	if depth > -1 {
		args = append(args, "-d", strconv.Itoa(depth))
	}

	if recurse {
		args = append(args, "-r")
	}

	if filter != "" {
		args = append(args, filter)
	}
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

	return datasets, nil
}

func propsSlice(properties map[string]string) []string {
	args := make([]string, 0, len(properties)*3)
	for k, v := range properties {
		args = append(args, "-o")
		args = append(args, fmt.Sprintf("%s=%s", k, v))
	}
	return args
}

func (z *Zpool) parseLine(line []string) error {
	if len(line) != len(ZpoolPropList) {
		return errors.New("Zpool output not what is expected on" +
			"this platform")
	}
	setString(&z.Name, line[0])
	setString(&z.Health, line[1])
	setString(&z.Allocated, line[2])
	setString(&z.Size, line[3])
	setString(&z.Free, line[4])

	return nil
}

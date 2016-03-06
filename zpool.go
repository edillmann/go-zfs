package zfs

import (
	"strings"
)

// ZFS zpool states, which can indicate if a pool is online, offline,
// degraded, etc.  More information regarding zpool states can be found here:
// https://docs.oracle.com/cd/E19253-01/819-5461/gamno/index.html.
const (
	ZpoolOnline   = "ONLINE"
	ZpoolDegraded = "DEGRADED"
	ZpoolFaulted  = "FAULTED"
	ZpoolOffline  = "OFFLINE"
	ZpoolUnavail  = "UNAVAIL"
	ZpoolRemoved  = "REMOVED"
)

// Zpool is a ZFS zpool.  A pool is a top-level structure in ZFS, and can
// contain many descendent datasets.
type Zpool struct {
	zfsh      *ZfsH
	Name      string
	Health    string
	Allocated string
	Size      string
	Free      string
}

// zpool is a helper function to wrap typical calls to zpool.
func (z *ZfsH) zpool(arg ...string) ([][]string, error) {
	c := &command{
		Command: "zpool",
		zh: z,
	}
	return c.Run(arg...)
}

// GetZpool retrieves a single ZFS zpool by name.
func (z *ZfsH) GetZpool(name string) (*Zpool, error) {
	out, err := z.zpool("list", "-o", strings.Join(ZpoolPropList, ","), name)
	if err != nil {
		return nil, err
	}

	// there is no -H
	out = out[1:]

	zp := &Zpool{Name: name}
	for _, line := range out {
		if err := zp.parseLine(line); err != nil {
			return nil, err
		}
	}
	return zp, nil
}

// CreateZpool creates a new ZFS zpool with the specified name, properties,
// and optional arguments.
// A full list of available ZFS properties and command-line arguments may be
// found here: https://www.freebsd.org/cgi/man.cgi?zfs(8).
func (z *ZfsH) CreateZpool(name string, properties map[string]string, args ...string) (*Zpool, error) {
	cli := make([]string, 1, 4)
	cli[0] = "create"
	if properties != nil {
		cli = append(cli, propsSlice(properties)...)
	}
	cli = append(cli, name)
	cli = append(cli, args...)
	_, err := z.zpool(cli...)
	if err != nil {
		return nil, err
	}

	return &Zpool{Name: name}, nil
}

// Destroy destroys a ZFS zpool by name.
func (z *ZfsH) DestroyZpool(zp *Zpool) error {
	_, err := z.zpool("destroy", zp.Name)
	return err
}

// ListZpools list all ZFS zpools accessible on the current system.
func (z *ZfsH) ListZpools() ([]*Zpool, error) {
	args := []string{"list", "-Ho", "name"}
	out, err := z.zpool(args...)
	if err != nil {
		return nil, err
	}

	var pools []*Zpool

	for _, line := range out {
		zp, err := z.GetZpool(line[0])
		if err != nil {
			return nil, err
		}
		pools = append(pools, zp)
	}
	return pools, nil
}

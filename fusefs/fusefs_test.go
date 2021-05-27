package fusefs

import (
	"context"
	"io/ioutil"
	"runtime"
	"sort"
	"testing"
	"testing/fstest"
	"time"

	"github.com/encoredev/fsutil/overlayfs"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestWorkspaceBasic(t *testing.T) {
	dir := t.TempDir()

	base := fstest.MapFS{
		"foo":     {Mode: 0755},
		"bar/baz": {Mode: 0755},
	}
	fsys := overlayfs.New(base)
	fs := New(fsys, nil)

	srv, err := fusefs.Mount(dir, fs.Root(), &fusefs.Options{
		MountOptions: fuse.MountOptions{
			Debug: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Unmount()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dirs, err := readDir(dir)
	if err != nil {
		t.Fatal(err)
	} else if len(dirs) != 2 || dirs[0] != "bar/" || dirs[1] != "foo" {
		t.Fatalf("got dirs %+v, want %+v", dirs, []string{"bar/", "foo"})
	}

	if err := fsys.Remove("foo"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Invalidate(ctx, "foo", -1); err != nil {
		if runtime.GOOS != "darwin" {
			t.Fatal(err)
		}
	}

	dirs, err = readDir(dir)
	if err != nil {
		t.Fatal(err)
	} else if len(dirs) != 1 || dirs[0] != "bar/" {
		t.Fatalf("got dirs %+v, want %+v", dirs, []string{"bar/"})
	}
}

func readDir(dir string) ([]string, error) {
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			name += "/"
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

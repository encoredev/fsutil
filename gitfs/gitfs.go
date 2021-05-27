package gitfs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"golang.org/x/sync/singleflight"
)

type Store interface {
	GetTree(context.Context, plumbing.Hash) (*object.Tree, error)
	GetBlob(context.Context, plumbing.Hash) (*object.Blob, error)
}

func New(s Store, tree plumbing.Hash) *FS {
	fs := &FS{
		s:     s,
		trees: make(map[plumbing.Hash]*object.Tree),
		blobs: make(map[plumbing.Hash]*object.Blob),
	}
	fs.root = &node{fs: fs, hash: tree, name: ".", mode: filemode.Dir}
	return fs
}

type FS struct {
	s    Store
	root *node
	g    singleflight.Group

	mu    sync.RWMutex
	trees map[plumbing.Hash]*object.Tree
	blobs map[plumbing.Hash]*object.Blob
}

var _ fs.FS = (*FS)(nil)

func (fsys *FS) Open(path string) (fs.File, error) {
	n, err := fsys.resolve(path)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: path, Err: err}
	}
	return n.File()
}

func (fsys *FS) resolve(path string) (*node, error) {
	n := fsys.root
	if path == "." {
		return n, nil
	}
	it := pathIter(path)
	for {
		var err error
		name, more := it.Next()
		n, err = n.Child(name)
		if err != nil {
			return nil, err
		} else if !more {
			return n, nil
		}
	}
}

type node struct {
	fs   *FS
	hash plumbing.Hash
	name string
	mode filemode.FileMode
}

func (n *node) File() (fs.File, error) {
	if n.mode == filemode.Dir {
		tree, err := n.fs.tree(n.hash)
		if err != nil {
			return nil, err
		}
		return &dirFile{n: n, tree: tree}, nil
	}
	blob, err := n.fs.blob(n.hash)
	if err != nil {
		return nil, err
	}
	return &file{n: n, blob: blob}, nil
}

func (n *node) Child(name string) (*node, error) {
	if n.mode != filemode.Dir {
		return nil, fs.ErrNotExist
	}
	tree, err := n.fs.tree(n.hash)
	if err != nil {
		return nil, err
	}
	for _, e := range tree.Entries {
		if e.Name == name {
			return &node{fs: n.fs, hash: e.Hash, name: e.Name, mode: e.Mode}, nil
		}
	}
	return nil, fs.ErrNotExist
}

// tree fetches and caches the git tree with the given hash.
func (c *FS) tree(h plumbing.Hash) (*object.Tree, error) {
	c.mu.RLock()
	t, ok := c.trees[h]
	c.mu.RUnlock()
	if ok {
		return t, nil
	}

	key := "tree:" + h.String()
	res, err, _ := c.g.Do(key, func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t, err := c.s.GetTree(ctx, h)
		if err == nil {
			c.mu.Lock()
			c.trees[h] = t
			c.mu.Unlock()
		}
		return t, err
	})
	if err != nil {
		return nil, err
	}
	return res.(*object.Tree), nil
}

// blob fetches and caches the git blob with the given hash.
func (c *FS) blob(h plumbing.Hash) (*object.Blob, error) {
	c.mu.RLock()
	b, ok := c.blobs[h]
	c.mu.RUnlock()
	if ok {
		return b, nil
	}

	key := "blob:" + h.String()
	res, err, _ := c.g.Do(key, func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		b, err := c.s.GetBlob(ctx, h)
		if err == nil {
			c.mu.Lock()
			c.blobs[h] = b
			c.mu.Unlock()
		}
		return b, err
	})
	if err != nil {
		return nil, err
	}
	return res.(*object.Blob), nil
}

type file struct {
	n    *node
	blob *object.Blob
	rc   io.ReadCloser
}

var _ fs.File = (*file)(nil)

func (f *file) Stat() (fs.FileInfo, error) {
	return &fileInfo{
		name: f.n.name,
		mode: f.n.mode,
		size: f.blob.Size,
	}, nil
}

func (f *file) Read(p []byte) (int, error) {
	if f.rc == nil {
		var err error
		f.rc, err = f.blob.Reader()
		if err != nil {
			return 0, err
		}
	}
	return f.rc.Read(p)
}

func (f *file) Close() error {
	if f.rc != nil {
		return f.rc.Close()
	}
	return nil
}

type dirFile struct {
	n    *node
	tree *object.Tree
	idx  int
}

var _ fs.ReadDirFile = (*dirFile)(nil)

func (d *dirFile) Stat() (fs.FileInfo, error) {
	return &fileInfo{
		name: d.n.name,
		mode: d.n.mode,
		size: 0,
	}, nil
}

func (d *dirFile) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("not a file")
}

func (d *dirFile) Close() error {
	return nil
}

func (d *dirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	ents := d.tree.Entries[d.idx:]
	if n >= 0 && n < len(ents) {
		ents = ents[:n]
	}
	entries := make([]fs.DirEntry, len(ents))
	for i, e := range ents {
		entries[i] = &dirEntry{fs: d.n.fs, e: e}
	}
	d.idx += len(entries)
	if n >= 0 && len(entries) == 0 {
		return nil, io.EOF
	}
	return entries, nil
}

// pathIter iterates over paths, returning path segments in order.
type pathIter string

func (it *pathIter) Next() (name string, more bool) {
	s := string(*it)

	idx := strings.IndexRune(s, '/')
	if idx == -1 {
		return s, false
	}
	segment := s[:idx]
	*it = pathIter(s[idx+1:])
	return segment, true
}

type fileInfo struct {
	name string
	size int64
	mode filemode.FileMode
}

func (fi *fileInfo) Name() string {
	return fi.name
}

func (fi *fileInfo) Size() int64 {
	return fi.size
}

func (fi *fileInfo) Mode() fs.FileMode {
	mode, err := fi.mode.ToOSFileMode()
	if err != nil {
		mode = fs.ModeIrregular
	}
	return mode
}

func (fi *fileInfo) ModTime() time.Time {
	return time.Time{}
}

func (fi *fileInfo) IsDir() bool {
	return fi.mode == filemode.Dir
}

func (fi *fileInfo) Sys() interface{} {
	return nil
}

type dirEntry struct {
	fs *FS
	e  object.TreeEntry
}

var _ fs.DirEntry = (*dirEntry)(nil)

func (e *dirEntry) Name() string { return e.e.Name }
func (e *dirEntry) IsDir() bool  { return e.e.Mode == filemode.Dir }
func (e *dirEntry) Type() fs.FileMode {
	mode, err := e.e.Mode.ToOSFileMode()
	if err != nil {
		mode = fs.ModeIrregular
	}
	return mode.Type()
}
func (e *dirEntry) Info() (fs.FileInfo, error) {
	fi := &fileInfo{
		name: e.e.Name,
		mode: e.e.Mode,
	}
	if !e.IsDir() {
		// Resolve blob size
		blob, err := e.fs.blob(e.e.Hash)
		if err != nil {
			return nil, err
		}
		fi.size = blob.Size
	}
	return fi, nil
}

package overlayfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	pathpkg "path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	_ fs.ReadDirFS = (*FS)(nil)
)

type FS struct {
	s *storage
}

func New(src fs.FS) *FS {
	return &FS{s: newStorage(src)}
}

func (fs *FS) Create(filename string) (fs.File, error) {
	return fs.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (fsys *FS) Open(filename string) (fs.File, error) {
	return fsys.OpenFile(filename, os.O_RDONLY, 0)
}

func (fsys *FS) OpenFile(filename string, flag int, perm os.FileMode) (f fs.File, err error) {
	defer func() {
		if err != nil {
			err = &os.PathError{Op: "open", Path: filename, Err: err}
		}
	}()

	nod, err := fsys.s.Get(filename)
	if os.IsNotExist(err) {
		if !isCreate(flag) {
			return nil, err
		}
		nod, err = fsys.s.New(filename, perm)
	}
	if err != nil {
		return nil, err
	}

	// Is it a directory?
	if nod.mode.IsDir() {
		return nod.DirFile()
	}

	file, err := nod.File()
	if err != nil {
		return nil, err
	}
	return file.Handle(filename, flag, perm), nil
}

func (fs *FS) Stat(filename string) (fi fs.FileInfo, err error) {
	defer func() {
		if err != nil {
			err = &os.PathError{Op: "stat", Path: filename, Err: err}
		}
	}()

	nod, err := fs.s.Get(filename)
	if err != nil {
		return nil, err
	}

	return nod.Stat()
}

func (fs *FS) Rename(oldpath, newpath string) error {
	err := fs.s.Rename(oldpath, newpath)
	if err != nil {
		return &os.PathError{Op: "rename", Path: oldpath, Err: err}
	}
	return nil
}

func (fs *FS) Remove(filename string) error {
	err := fs.s.Remove(filename)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return &os.PathError{Op: "remove", Path: filename, Err: err}
	}
	return nil
}

func (fs *FS) Join(elem ...string) string {
	return pathpkg.Join(elem...)
}

func (fs *FS) ReadDir(path string) ([]fs.DirEntry, error) {
	entries, err := fs.s.ReadDir(path)
	if err != nil {
		return nil, &os.PathError{Op: "readdir", Path: path, Err: err}
	}
	return entries, nil
}

func (fs *FS) MkdirAll(dirname string, perm os.FileMode) error {
	err := fs.s.MkdirAll(dirname, perm)
	if err != nil {
		return &os.PathError{Op: "mkdirall", Path: dirname, Err: err}
	}
	return nil
}

func newStorage(src fs.FS) *storage {
	fsys := &storage{src: src}
	fsys.root = newNode(fsys, ".", fs.ModeDir, rootInode)
	return fsys
}

type storage struct {
	root  *node
	src   fs.FS
	inode int64 // atomically incremented
}

// Get retrieves the tree entry at path.
func (s *storage) Get(path string) (*node, error) {
	it := pathIter(path)
	nod := s.root
	if path == "." {
		return nod, nil
	}

	for {
		name, more := it.Next()
		var err error
		nod, err = nod.Child(name)
		if err != nil {
			return nil, err
		} else if !more {
			break
		}
	}
	return nod, nil
}

// New creates a new node at path.
func (s *storage) New(path string, mode fs.FileMode) (*node, error) {
	path, err := clean(path)
	if err != nil {
		return nil, err
	}

	parent := pathpkg.Dir(path)
	parentNod, err := s.Get(parent)
	if err != nil {
		return nil, err
	} else if !parentNod.mode.IsDir() {
		return nil, fs.ErrNotExist
	}

	nod := s.mkNode(path, mode)
	nod.MarkModified()
	if mode.IsDir() {
		nod.children = &nodes{nod: nod}
	} else {
		nod.file = &file{nod: nod}
	}
	err = parentNod.AddChild(nod, pathpkg.Base(path))
	return nod, err
}

func (s *storage) ReadDir(path string) ([]fs.DirEntry, error) {
	path, err := clean(path)
	if err != nil {
		return nil, err
	}
	nod, err := s.Get(path)
	if err != nil {
		return nil, err
	}

	if !nod.mode.IsDir() {
		return nil, fmt.Errorf("not a dir")
	}
	return nod.ReadDir()
}

func (s *storage) Rename(oldpath, newpath string) error {
	oldpath, err := clean(oldpath)
	if err != nil {
		return err
	}
	newpath, err = clean(newpath)
	if err != nil {
		return err
	}

	// Cannot move a node to become a child of itself.
	if strings.HasPrefix(newpath, oldpath+"/") {
		return fmt.Errorf("a path is a parent of the other")
	}

	oldParentPath := pathpkg.Dir(oldpath)
	oldParent, err := s.Get(oldParentPath)
	if err != nil {
		return err
	}
	oldChildren, err := oldParent.Children()
	if err != nil {
		return err
	}

	newParentPath := pathpkg.Dir(newpath)
	newParent, err := s.Get(newParentPath)
	if err != nil {
		return err
	} else if !newParent.mode.IsDir() {
		return fmt.Errorf("parent is not a dir")
	}

	newChildren, err := newParent.Children()
	if err != nil {
		return err
	}
	oldName := pathpkg.Base(oldpath)
	newName := pathpkg.Base(newpath)
	return move(oldName, newName, oldChildren, newChildren)
}

// Remove removes the node at path.
// It reports os.ErrNotExist if the node does not exist.
func (s *storage) Remove(path string) error {
	path, err := clean(path)
	if err != nil {
		return err
	} else if path == "/" {
		return fmt.Errorf("cannot delete root")
	}

	parentPath := pathpkg.Dir(path)
	parent, err := s.Get(parentPath)
	if err != nil {
		return err
	} else if !parent.mode.IsDir() {
		return fs.ErrNotExist
	}
	ch, err := parent.Children()
	if err != nil {
		return err
	}
	if !ch.Delete(pathpkg.Base(path)) {
		return fs.ErrNotExist
	}
	return nil
}

func (s *storage) MkdirAll(path string, perm os.FileMode) error {
	path, err := clean(path)
	if err != nil {
		return err
	}
	if !perm.IsDir() {
		return fmt.Errorf("cannot mkdir node with non-dir mode: %s", perm)
	}

	it := pathIter(path)
	nod := s.root
	for {
		name, more := it.Next()
		if !nod.mode.IsDir() {
			return fs.ErrExist
		}
		child, err := nod.Child(name)
		if errors.Is(err, fs.ErrNotExist) {
			// Create an empty directory
			child = s.mkNode(pathpkg.Join(nod.path, name), fs.ModeDir)
			child.children = &nodes{nod: child}
			if err := nod.AddChild(child, name); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		nod = child
		if !more {
			break
		}
	}
	return nil
}

/*
func (s *FS) WriteTree(ctx context.Context) (plumbing.Hash, bool, error) {
	return s.root.WriteObj(ctx)
}
*/

// node represents a child node in a tree.
type node struct {
	// immutable
	path  string
	mode  fs.FileMode
	fs    *storage
	inode Inode

	// mutable
	modified atomic.Value // bool
	cacheMu  sync.Mutex   // guard setting/getting children and file
	children *nodes       // lazily populated for dirs
	file     *file        // lazily populated for files
}

func (fs *storage) mkNode(path string, mode fs.FileMode) *node {
	inode := Inode(atomic.AddInt64(&fs.inode, 1))
	return newNode(fs, path, mode, inode)
}

func newNode(fsys *storage, path string, mode fs.FileMode, inode Inode) *node {
	n := &node{
		fs:    fsys,
		path:  path,
		mode:  mode,
		inode: inode,
	}
	n.modified.Store(false)
	return n
}

func (n *node) MarkModified() {
	n.modified.Store(true)
}

func (n *node) Modified() bool {
	return n.modified.Load().(bool)
}

// Children fetches the child nodes for the given entry.
func (n *node) Children() (*nodes, error) {
	if !n.mode.IsDir() {
		return nil, fmt.Errorf("not a directory")
	}
	n.cacheMu.Lock()
	ch := n.children
	n.cacheMu.Unlock()
	if ch != nil {
		return ch, nil
	}

	entries, err := fs.ReadDir(n.fs.src, n.path)
	if err != nil {
		return nil, err
	}

	// Build the nodes. We can use append since we're entries in tree order already.
	ch = &nodes{nod: n}
	for _, e := range entries {
		nod := n.fs.mkNode(pathpkg.Join(n.path, e.Name()), e.Type())
		ch.append(nod, e.Name(), false)
	}

	n.cacheMu.Lock()
	if n.children == nil {
		n.children = ch
	}
	n.cacheMu.Unlock()
	return ch, nil
}

// File fetches the file object for the given node.
func (n *node) File() (*file, error) {
	if n.mode.IsDir() {
		return nil, fmt.Errorf("not a file")
	}
	n.cacheMu.Lock()
	f := n.file
	n.cacheMu.Unlock()
	if f != nil {
		return f, nil
	}

	data, err := fs.ReadFile(n.fs.src, n.path)
	if err == nil {
		n.cacheMu.Lock()
		if n.file == nil {
			n.file = &file{nod: n, data: data}
		}
		n.cacheMu.Unlock()
	}
	return n.file, err
}

func (n *node) DirFile() (*dirFile, error) {
	if !n.mode.IsDir() {
		return nil, fmt.Errorf("not a dir")
	}
	return &dirFile{n: n}, nil
}

// Child retrieves the child with the given name.
// If the child does not exist or n is not a dir, it reports ErrNotExist.
func (n *node) Child(name string) (*node, error) {
	if !n.mode.IsDir() {
		return nil, fs.ErrNotExist
	}

	ch, err := n.Children()
	if err != nil {
		return nil, err
	}

	child, ok := ch.Get(name)
	if !ok {
		return nil, fs.ErrNotExist
	}
	return child, nil
}

// AddChild adds a child node with the given name.
// If a child with the given name already exists it reports os.ErrExist.
func (n *node) AddChild(child *node, name string) error {
	if !n.mode.IsDir() {
		return fmt.Errorf("node is not a dir")
	} else if child == n {
		return fmt.Errorf("cannot add self as child node")
	}
	ch, err := n.Children()
	if err != nil {
		return err
	}
	if !ch.Insert(child, name) {
		return fs.ErrExist
	}
	return nil
}

func (n *node) ReadDir() ([]fs.DirEntry, error) {
	ch, err := n.Children()
	if err != nil {
		return nil, err
	}

	var fis []fs.DirEntry
	err = ch.Iterate(func(n *node, name string) error {
		fis = append(fis, &dirEntry{node: n})
		return nil
	})
	return fis, err
}

func (n *node) Stat() (fs.FileInfo, error) {
	size := int64(0)
	if !n.mode.IsDir() {
		f, err := n.File()
		if err != nil {
			return nil, err
		}
		size = f.Size()
	}

	return &fileInfo{
		name:  pathpkg.Base(n.path),
		mode:  n.mode,
		size:  size,
		inode: n.inode,
	}, nil
}

/* TODO
func (n *node) WriteObj(ctx context.Context) (hash plumbing.Hash, written bool, err error) {
	if !n.Mode.IsDir() {
		// For blobs, we can trust the hash for modification detection.
		if hash = n.Hash(); hash != plumbing.ZeroHash {
			return hash, false, nil
		}

		f, err := n.File(ctx)
		if err != nil {
			return plumbing.ZeroHash, false, err
		}
		fh := f.Handle("", 0, 0)
		defer fh.Close()

		obj := n.objs.s.NewEncodedObj()
		obj.SetType(plumbing.BlobObject)
		wc, err := obj.Writer()
		if err != nil {
			return plumbing.ZeroHash, false, err
		}
		_, err = io.Copy(wc, fh)
		if err2 := wc.Close(); err == nil {
			err = err2
		}
		if err == nil {
			hash, err = n.objs.s.WriteObj(ctx, obj)
			written = err == nil
		}
		return hash, written, err
	}

	// For directories, we can't trust the hash for modification detection
	// since we don't propagate modifications up the tree.
	// However, in the case where we have a hash and we haven't actually fetched
	// the children, we can trust it since we can't possibly have modified it.
	n.cacheMu.Lock()
	ch := n.children
	n.cacheMu.Unlock()

	if ch == nil {
		hash := n.Hash()
		if hash.IsZero() {
			panic("internal error: dir node with zero hash and uninitialized children")
		}
		return hash, false, nil
	}

	// Iterate over our children concurrently.
	t := &object.Tree{}
	var changed atomic.Value
	{
		g, gctx := errgroup.WithContext(ctx)
		idx := 0
		changed.Store(false)
		ch.Iterate(func(n *node, name string) error {
			// Reserve an index entry in the slice so we can write
			// to it concurrently without racing.
			t.Entries = append(t.Entries, object.TreeEntry{
				Name: name,
				Mode: n.Mode,
			})
			i := idx
			idx++

			g.Go(func() error {
				if err := ctx.Err(); err != nil {
					return err
				}

				hash, written, err := n.WriteObj(gctx)
				if err != nil {
					return err
				}
				t.Entries[i].Hash = hash
				if written {
					changed.Store(true)
				}
				return nil
			})
			return nil
		})

		if err := g.Wait(); err != nil {
			return plumbing.ZeroHash, false, err
		}
	}

	// See if something changed and write the tree object if so.
	hash = n.Hash()
	if !changed.Load().(bool) && !hash.IsZero() {
		return hash, false, nil
	}
	obj := n.objs.s.NewEncodedObj()
	if err = t.Encode(obj); err != nil {
		return plumbing.ZeroHash, false, err
	}
	hash, err = n.objs.s.WriteObj(ctx, obj)
	return hash, err == nil, err
}
*/

// nodes exposes operations for efficiently searching and mutating
// a slice of nodes while maintaining git's tree sort order.
type nodes struct {
	mu      sync.RWMutex
	gen     int // modification generation
	nod     *node
	nodes   []*node
	names   []string
	deleted []bool
}

// Get gets the node with the given name.
func (n *nodes) Get(name string) (*node, bool) {
	// git sorts directories differently from files (see sortName below),
	// which means we don't know in advance where to find the nond
	// unless we know whether it's a file or directory.
	//
	// However, we can leverage the fact that sort.Search reports
	// where the item would be inserted, which we know will be just before
	// the directory entry.
	//
	// The algorithm is thus: search for the file, and from that index
	// loop until we either find the file, the directory, or give up
	// if we iterate beyond it.
	n.mu.RLock()
	defer n.mu.RUnlock()
	idx := sort.Search(len(n.nodes), func(i int) bool {
		return n.sortName(i) >= name
	})
	dirKey := name + "/"
	for i := idx; i < len(n.nodes); i++ {
		if n.names[i] == name {
			return n.nodes[i], !n.deleted[i]
		} else if n.sortName(i) > dirKey {
			// We've gone past the directory; stop searching
			break
		}
	}
	return nil, false
}

// Insert inserts the given node, which mutates n.
// If the entry already exists in the slice, it reports false.
func (n *nodes) Insert(nod *node, name string) bool {
	// Use optimistic concurrency: find where the node is with a read lock
	// and acquire a write lock to write. Retry if the node changed place.

	sortKey := sortName(name, nod.mode)
	for {
		// Find where to insert it
		n.mu.RLock()
		idx := sort.Search(len(n.nodes), func(i int) bool {
			return n.sortName(i) >= sortKey
		})
		gen := n.gen
		n.mu.RUnlock()

		n.mu.Lock()
		if n.gen != gen {
			// Concurrent modification; try again
			n.mu.Unlock()
			continue
		}
		// Generation is the same; we can write
		defer n.mu.Unlock()

		if idx < len(n.nodes) && n.names[idx] == name {
			// If it's deleted, overwrite it; otherwise report false.
			if n.deleted[idx] {
				n.nodes[idx] = nod
				n.deleted[idx] = false
				// This does not change gen since we're not modifying elem positions.
				n.nod.MarkModified()
				return true
			}
			return false
		}
		n.nodes = append(n.nodes[:idx], append([]*node{nod}, n.nodes[idx:]...)...)
		n.names = append(n.names[:idx], append([]string{name}, n.names[idx:]...)...)
		n.deleted = append(n.deleted[:idx], append([]bool{false}, n.deleted[idx:]...)...)
		n.gen++
		n.nod.MarkModified()
		return true
	}
}

// Delete deletes the node with the given name.
// It reports whether the node was deleted (that is, it existed before the call).
func (n *nodes) Delete(name string) bool {
	// Use optimistic concurrency: find where the node is with a read lock
	// and acquire a write lock to write. Retry if the node changed place.

	// doDelete acquires a write lock, attempts to perform the delete,
	// and reports the result.
	doDelete := func(i, gen int) (deleted, retry bool) {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.gen != gen {
			return false, true
		}
		wasDeleted := n.deleted[i]
		n.deleted[i] = true
		return !wasDeleted, false
	}

RetryLoop:
	for {
		n.mu.RLock()
		idx := sort.Search(len(n.nodes), func(i int) bool {
			return n.sortName(i) >= name
		})

		dirKey := name + "/"
		for i := idx; i < len(n.nodes); i++ {
			if n.names[i] == name {
				gen := n.gen
				n.mu.RUnlock()
				if deleted, retry := doDelete(i, gen); !retry {
					if deleted {
						n.nod.MarkModified()
					}
					return deleted
				}
				continue RetryLoop
			} else if n.sortName(i) > dirKey {
				break
			}
		}

		// We couldn't find the node; we're done
		n.mu.RUnlock()
		return false
	}
}

// Iterate iterates over the child nodes, calling fn for each one.
// Concurrent insertions are not guaranteed to be iterated over.
func (n *nodes) Iterate(fn func(nod *node, name string) error) error {
	n.mu.RLock()
	for i := 0; ; i++ {
		// Precondition: n.mu is read locked

		if i >= len(n.nodes) {
			n.mu.RUnlock()
			return nil
		}

		if !n.deleted[i] {
			nod := n.nodes[i]
			name := n.names[i]

			n.mu.RUnlock()
			if err := fn(nod, name); err != nil {
				return err
			}
			n.mu.RLock()
		}
	}
}

// append appends the given data to n.
// It does not enforce order, so it must only be called in tree order!
// It does not acquire n.mu.
func (n *nodes) append(nod *node, name string, deleted bool) {
	n.nodes = append(n.nodes, nod)
	n.names = append(n.names, name)
	n.deleted = append(n.deleted, deleted)
	n.nod.MarkModified()
}

// move orchestrates an atomic move of the node with the given name
// between parent nodes from and to.
func move(oldname, newname string, from, to *nodes) error {
	// doMove acquires a write lock on both nodes,
	// attempts to perform the move,
	// and reports the result.
	errRetry := errors.New("retry")
	doMove := func(i, gen int) error {
		from.mu.Lock()
		defer from.mu.Unlock()
		if from.gen != gen {
			return errRetry
		}
		if from.deleted[i] {
			return os.ErrNotExist
		}

		// If the nodes are the same, just change the name field.
		if from == to {
			from.names[i] = newname
			from.nod.MarkModified()
			return nil
		}

		// Otherwise insert and remove.
		nod := from.nodes[i]
		if !to.Insert(nod, newname) {
			return os.ErrExist
		}
		from.nod.MarkModified()
		to.nod.MarkModified()
		from.deleted[i] = true
		return nil
	}

RetryLoop:
	for {
		from.mu.RLock()
		idx := sort.Search(len(from.nodes), func(i int) bool {
			return from.sortName(i) >= oldname
		})

		dirKey := oldname + "/"
		for i := idx; i < len(from.nodes); i++ {
			if from.names[i] == oldname {
				gen := from.gen
				from.mu.RUnlock()
				if err := doMove(i, gen); err != errRetry {
					return err
				}
				continue RetryLoop
			} else if from.sortName(i) > dirKey {
				// We've gone past the directory; stop searching
				break
			}
		}

		// We couldn't find the node; we're done
		from.mu.RUnlock()
		return os.ErrNotExist
	}
}

// sortName returns the sortNName of the node with index i.
// n.mu is assumed to be held.
func (n *nodes) sortName(i int) string {
	return sortName(n.names[i], n.nodes[i].mode)
}

func sortName(name string, mode fs.FileMode) string {
	if mode.IsDir() {
		return name + "/"
	}
	return name
}

type file struct {
	mu   sync.RWMutex
	nod  *node
	data []byte
}

func (f *file) Size() int64 {
	f.mu.RLock()
	sz := int64(len(f.data))
	f.mu.RUnlock()
	return sz
}

func (f *file) Handle(filename string, flag int, mode fs.FileMode) fs.File {
	h := &fileHandle{
		f:    f,
		mode: mode,
		flag: flag,
	}

	if isAppend(flag) {
		h.position = f.Size()
	}
	if isTruncate(flag) {
		h.f.Truncate(0)
	}
	return h
}

func (f *file) Truncate(size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nod.MarkModified()

	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	} else if more := int(size) - len(f.data); more > 0 {
		f.data = append(f.data, make([]byte, more)...)
	}
}

func (f *file) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("writeat: negative offset")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.nod.MarkModified()

	prev := len(f.data)

	diff := int(off) - prev
	if diff > 0 {
		f.data = append(f.data, make([]byte, diff)...)
	}

	f.data = append(f.data[:off], p...)
	if len(f.data) < prev {
		f.data = f.data[:prev]
	}

	return len(p), nil
}

func (f *file) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("readat: negative offset")
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	size := int64(len(f.data))
	if off >= size {
		return 0, io.EOF
	}

	l := int64(len(p))
	if off+l > size {
		l = size - off
	}

	btr := f.data[off : off+l]
	if len(btr) < len(p) {
		err = io.EOF
	}
	n = copy(p, btr)
	return
}

type fileHandle struct {
	f        *file
	position int64
	flag     int
	mode     os.FileMode
	closed   bool
}

func (h *fileHandle) Name() string {
	return pathpkg.Base(h.f.nod.path)
}

func (h *fileHandle) Read(p []byte) (int, error) {
	n, err := h.f.ReadAt(p, h.position)
	h.position += int64(n)

	if err == io.EOF && n != 0 {
		err = nil
	}
	return n, err
}

func (h *fileHandle) ReadAt(p []byte, off int64) (int, error) {
	if h.closed {
		return 0, os.ErrClosed
	}

	if !isReadAndWrite(h.flag) && !isReadOnly(h.flag) {
		return 0, errors.New("read not supported")
	}
	n, err := h.f.ReadAt(p, off)
	return n, err
}

func (h *fileHandle) Seek(offset int64, whence int) (int64, error) {
	if h.closed {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekCurrent:
		h.position += offset
	case io.SeekStart:
		h.position = offset
	case io.SeekEnd:
		h.position = h.f.Size() + offset
	}

	return h.position, nil
}

func (h *fileHandle) Write(p []byte) (int, error) {
	if h.closed {
		return 0, os.ErrClosed
	}

	if !isReadAndWrite(h.flag) && !isWriteOnly(h.flag) {
		return 0, errors.New("write not supported")
	}

	n, err := h.f.WriteAt(p, h.position)
	h.position += int64(n)
	return n, err
}

func (h *fileHandle) WriteAt(p []byte, off int64) (int, error) {
	if h.closed {
		return 0, os.ErrClosed
	}

	if !isReadAndWrite(h.flag) && !isWriteOnly(h.flag) {
		return 0, errors.New("write not supported")
	}

	return h.f.WriteAt(p, off)
}

func (h *fileHandle) Close() error {
	if h.closed {
		return os.ErrClosed
	}
	h.closed = true
	return nil
}

func (h *fileHandle) Truncate(size int64) error {
	h.f.Truncate(size)
	return nil
}

func (h *fileHandle) Stat() (fs.FileInfo, error) {
	size := int64(0)
	if !h.mode.IsDir() {
		size = h.f.Size()
	}
	return &fileInfo{
		name:  pathpkg.Base(h.f.nod.path),
		mode:  h.mode,
		size:  size,
		inode: h.f.nod.inode,
	}, nil
}

func (h *fileHandle) Lock() error {
	return nil
}

func (h *fileHandle) Unlock() error {
	return nil
}

type dirFile struct {
	n       *node
	entries []fs.DirEntry
}

var _ fs.ReadDirFile = (*dirFile)(nil)

func (d *dirFile) Close() error { return nil }
func (d *dirFile) Stat() (fs.FileInfo, error) {
	return &fileInfo{
		name:  pathpkg.Base(d.n.path),
		mode:  d.n.mode,
		size:  0,
		inode: d.n.inode,
	}, nil
}

func (d *dirFile) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.n.path, Err: fmt.Errorf("not a file")}
}

func (d *dirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.entries == nil {
		d.entries = []fs.DirEntry{}
		nodes, err := d.n.Children()
		if err == nil {
			err = nodes.Iterate(func(n *node, name string) error {
				d.entries = append(d.entries, &dirEntry{node: n})
				return nil
			})
		}
		if err != nil {
			return nil, &fs.PathError{
				Op:   "readdir",
				Path: d.n.path,
				Err:  err,
			}
		}
	}

	entries := d.entries
	if n >= 0 && len(d.entries) > n {
		entries = d.entries[:n]
	}
	d.entries = d.entries[len(entries):]
	if n >= 0 && len(entries) == 0 {
		return nil, io.EOF
	}
	return entries, nil
}

type Inode int64

const rootInode = 2

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

// clean checks the path to make sure it's absolute, and cleans it.
func clean(path string) (string, error) {
	return pathpkg.Clean(path), nil
}

type fileInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	inode Inode
	mod   time.Time
}

func (fi *fileInfo) Name() string {
	return fi.name
}

func (fi *fileInfo) Size() int64 {
	return fi.size
}

func (fi *fileInfo) Mode() os.FileMode {
	return fi.mode
}

func (fi *fileInfo) ModTime() time.Time {
	return fi.mod
}

func (fi *fileInfo) IsDir() bool {
	return fi.mode.IsDir()
}

func (fi *fileInfo) Sys() interface{} {
	return fi.inode
}

type dirEntry struct {
	node *node
}

var _ fs.DirEntry = (*dirEntry)(nil)

func (e *dirEntry) Name() string               { return pathpkg.Base(e.node.path) }
func (e *dirEntry) IsDir() bool                { return e.node.mode.IsDir() }
func (e *dirEntry) Type() fs.FileMode          { return e.node.mode }
func (e *dirEntry) Info() (fs.FileInfo, error) { return e.node.Stat() }

func isCreate(flag int) bool {
	return flag&os.O_CREATE != 0
}

func isAppend(flag int) bool {
	return flag&os.O_APPEND != 0
}

func isTruncate(flag int) bool {
	return flag&os.O_TRUNC != 0
}

func isReadAndWrite(flag int) bool {
	return flag&os.O_RDWR != 0
}

func isWriteOnly(flag int) bool {
	return flag&os.O_WRONLY != 0
}

func isReadOnly(flag int) bool {
	return !isReadAndWrite(flag) && !isWriteOnly(flag)
}

func isSymlink(m os.FileMode) bool {
	return m&os.ModeSymlink != 0
}

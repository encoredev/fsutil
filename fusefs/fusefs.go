// Package fusefs adapts io/fs filesystems to work with go-fuse.
package fusefs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	pathpkg "path"
	"sync"
	"syscall"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// WriteFS is like fs.FS but also supports opening files for writing.
type WriteFS interface {
	fs.FS
	OpenFile(filename string, flag int, perm os.FileMode) (fs.File, error)
}

// SeekableFile is like fs.File but also supports Seek and ReadAt.
// Files returned by the adapted fs must implement SeekableFile to enable
// seeking and reading from an offset.
type SeekableFile interface {
	fs.File
	io.ReaderAt
	io.Seeker
}

// WritableFile is like fs.File but also supports WriteAt for writing.
// Files returned by a filesystem that implements WriteFS must implement WritableFile.
type WritableFile interface {
	fs.File
	io.WriterAt
}

func New(fsys fs.FS, logger *log.Logger) *FS {
	seq := &inodeSeq{}
	inodes := &inodeMap{seq: seq}
	_, writable := fsys.(WriteFS)
	fs := &FS{
		fsys:     fsys,
		inodes:   inodes,
		writable: writable,
		logger:   logger,
	}
	root := &dirNode{fs: fs, path: "."}
	fs.root = root
	return fs
}

type FS struct {
	root     *dirNode
	fsys     fs.FS
	inodes   *inodeMap
	writable bool
	logger   *log.Logger
}

func (fs *FS) Root() fusefs.InodeEmbedder {
	return fs.root
}

// Invalidate invalidates the cached inode at path, causing the kernel to fetch it again
// on next operation. If size is negative, this signals to invalidate the file handle itself.
// If size is zero or positive, it indicates to invalidate the content of the file.
func (fs *FS) Invalidate(ctx context.Context, path string, size int64) error {
	inodes := fs.inodes
	if size == -1 {
		dir := pathpkg.Dir(path)
		name := pathpkg.Base(path)
		if ino, ok := inodes.GetIfExists(dir); ok {
			inode := fs.root.NewInode(ctx, &dirNode{fs: fs, path: dir}, fusefs.StableAttr{
				Mode: fuse.S_IFDIR,
				Ino:  ino,
			})
			if errno := inode.NotifyEntry(name); errno != 0 {
				return fmt.Errorf("invalidate %s: %v", path, errno)
			}
		}
	} else {
		if ino, ok := inodes.GetIfExists(path); ok {
			inode := fs.root.NewInode(ctx, &dirNode{fs: fs, path: path}, fusefs.StableAttr{
				Mode: fuse.S_IFREG,
				Ino:  ino,
			})
			if errno := inode.NotifyContent(0, size); errno != 0 {
				return fmt.Errorf("invalidate %s: error %v", path, errno)
			}
		}
	}
	return nil
}

type inodeSeq struct {
	mu    sync.Mutex
	count uint64
}

func (seq *inodeSeq) Allocate() uint64 {
	seq.mu.Lock()
	defer seq.mu.Unlock()
	seq.count++
	return seq.count + 1 // start from 2
}

type inodeMap struct {
	seq  *inodeSeq
	mu   sync.RWMutex
	vals map[string]uint64
}

func (m *inodeMap) Get(path string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.vals == nil {
		m.vals = make(map[string]uint64)
	}
	if n, ok := m.vals[path]; ok {
		return n
	}
	n := m.seq.Allocate()
	m.vals[path] = n
	return n
}

func (m *inodeMap) GetIfExists(path string) (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.vals[path]
	return n, ok
}

type dirNode struct {
	fusefs.Inode
	fs   *FS
	path string
}

var (
	_ = (fusefs.InodeEmbedder)((*dirNode)(nil))
	_ = (fusefs.NodeLookuper)((*dirNode)(nil))
	_ = (fusefs.NodeOpener)((*dirNode)(nil))
	_ = (fusefs.NodeGetattrer)((*dirNode)(nil))
)

func (n *dirNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	dents, err := fs.ReadDir(n.fs.fsys, n.path)
	if os.IsNotExist(err) {
		return nil, syscall.ENOENT
	} else if err != nil {
		return nil, syscall.EIO
	}

	var entries []fuse.DirEntry
	for _, e := range dents {
		fullPath := pathpkg.Join(n.path, e.Name())
		entries = append(entries, fuse.DirEntry{
			Mode: uint32(e.Type()),
			Name: e.Name(),
			Ino:  n.fs.inodes.Get(fullPath),
		})
	}
	return fusefs.NewListDirStream(entries), 0
}

func fiMode(fi os.FileInfo) uint32 {
	if fi.IsDir() {
		return uint32(fuse.S_IFDIR | 0777)
	}
	return uint32(fuse.S_IFREG | 0666)
}

func (n *dirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	path := pathpkg.Join(n.path, name)
	fi, err := fs.Stat(n.fs.fsys, path)
	if os.IsNotExist(err) {
		return nil, syscall.ENOENT
	} else if err != nil {
		return nil, syscall.EIO
	}
	child := n.NewInode(ctx, &dirNode{fs: n.fs, path: path}, fusefs.StableAttr{
		Mode: fiMode(fi),
		Ino:  n.fs.inodes.Get(path),
	})
	return child, 0
}

func (n *dirNode) Open(ctx context.Context, openFlags uint32) (fh fusefs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	if !n.fs.writable && openFlags&(syscall.O_RDWR|syscall.O_WRONLY) != 0 {
		return nil, 0, syscall.EROFS
	}
	f, err := n.fs.fsys.(WriteFS).OpenFile(n.path, int(openFlags), 0)
	if os.IsNotExist(err) {
		return nil, 0, syscall.ENOENT
	} else if err != nil {
		return nil, 0, syscall.EIO
	}

	fuseFlags = fuse.FOPEN_DIRECT_IO
	_, seekable := f.(SeekableFile)
	if !seekable {
		fuseFlags |= fuse.FOPEN_NONSEEKABLE
	}

	return &fileHandle{fs: n.fs, f: f, path: n.path, seekable: seekable}, fuseFlags, 0
}

func (n *dirNode) Getattr(ctx context.Context, fh fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fi, err := fs.Stat(n.fs.fsys, n.path)
	if os.IsNotExist(err) {
		return syscall.ENOENT
	} else if err != nil {
		return syscall.EIO
	}
	out.Attr.Size = uint64(fi.Size())
	out.Attr.Mode = fiMode(fi)
	return 0
}

type fileHandle struct {
	fs       *FS
	f        fs.File
	seekable bool
	path     string
}

var (
	_ = (fusefs.FileReader)((*fileHandle)(nil))
	_ = (fusefs.FileWriter)((*fileHandle)(nil))
)

func (fh *fileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	var (
		n   int
		err error
	)
	if fh.seekable {
		n, err = fh.f.(SeekableFile).ReadAt(dest, off)
	} else {
		n, err = fh.f.Read(dest)
	}

	errno := syscall.Errno(0)
	if err != nil && err != io.EOF {
		if log := fh.fs.logger; log != nil {
			log.Printf("fusefs: read %s: %v", fh.path, err)
		}
		errno = syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), errno
}

func (fh *fileHandle) Write(ctx context.Context, buf []byte, off int64) (uint32, syscall.Errno) {
	n, err := fh.f.(WritableFile).WriteAt(buf, off)
	errno := syscall.Errno(0)
	if err != nil && err != io.EOF {
		if log := fh.fs.logger; log != nil {
			log.Printf("fusefs: write %s: %v", fh.path, err)
		}
		errno = syscall.EIO
	}
	return uint32(n), errno
}

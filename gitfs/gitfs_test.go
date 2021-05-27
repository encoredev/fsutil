package gitfs

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

func TestFS(t *testing.T) {
	c := cache.NewObjectLRUDefault()
	store := filesystem.NewStorage(osfs.New("./testdata/repo"), c)
	headTree := plumbing.NewHash("503bdb49d84130f66b10523295ce98abf7cb83d5")
	fsys := New(&fsStore{s: store}, headTree)
	err := fstest.TestFS(fsys, "README.md", ".gitignore", "board/board.go", "board/column.go", "card/card.go")
	if err != nil {
		t.Fatal(err)
	}
}

type fsStore struct {
	s *filesystem.Storage
}

func (s *fsStore) GetTree(ctx context.Context, h plumbing.Hash) (*object.Tree, error) {
	return object.GetTree(s.s, h)
}

func (s *fsStore) GetBlob(ctx context.Context, h plumbing.Hash) (*object.Blob, error) {
	return object.GetBlob(s.s, h)
}

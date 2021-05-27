package overlayfs

import (
	"testing"
	"testing/fstest"
)

func TestFS(t *testing.T) {
	underlying := fstest.MapFS{
		"moo": &fstest.MapFile{
			Data: []byte("test"),
			Mode: 0600,
		},
		"foo/bar": &fstest.MapFile{
			Data: []byte("test"),
			Mode: 0644,
		},
		"foo/baz": &fstest.MapFile{
			Data: []byte("test"),
			Mode: 0644,
		},
	}
	fsys := New(underlying)
	err := fstest.TestFS(fsys, "moo", "foo/bar", "foo/baz")
	if err != nil {
		t.Fatal(err)
	}
}

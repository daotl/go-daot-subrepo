// Package namespace introduces a namespace Datastore Shim, which basically
// mounts the entire child datastore under a prefix.
//
// Use the Wrap function to wrap a datastore with any Key prefix. For example:
//
//  import (
//    "fmt"
//
//    ds "github.com/daotl/go-datastore"
//    key "github.com/daotl/go-datastore/key"
//    nsds "github.com/daotl/go-datastore/namespace"
//  )
//
//  func main() {
//    mp := ds.NewMapDatastore()
//    ns := nsds.Wrap(mp, key.NewStrKey("/foo/bar"))
//
//    // in the Namespace Datastore:
//    ns.Put(key.NewStrKey("/beep"), "boop")
//    v2, _ := ns.Get(key.NewStrKey("/beep")) // v2 == "boop"
//
//    // and, in the underlying MapDatastore:
//    v3, _ := mp.Get(key.NewStrKey("/foo/bar/beep")) // v3 == "boop"
//  }
package namespace

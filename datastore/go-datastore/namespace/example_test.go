// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package namespace_test

import (
	"context"
	"fmt"

	ds "github.com/daotl/go-datastore"
	key "github.com/daotl/go-datastore/key"
	nsds "github.com/daotl/go-datastore/namespace"
)

func Example() {
	ctx := context.Background()

	mp, _ := ds.NewMapDatastore(key.KeyTypeString)
	ns := nsds.Wrap(mp, key.NewStrKey("/foo/bar"))

	k := key.NewStrKey("/beep")
	v := "boop"

	if err := ns.Put(ctx, k, []byte(v)); err != nil {
		panic(err)
	}
	fmt.Printf("ns.Put %s %s\n", k, v)

	v2, _ := ns.Get(ctx, k)
	fmt.Printf("ns.Get %s -> %s\n", k, v2)

	k3 := key.NewStrKey("/foo/bar/beep")
	v3, _ := mp.Get(ctx, k3)
	fmt.Printf("mp.Get %s -> %s\n", k3, v3)
	// Output:
	// ns.Put /beep boop
	// ns.Get /beep -> boop
	// mp.Get /foo/bar/beep -> boop
}

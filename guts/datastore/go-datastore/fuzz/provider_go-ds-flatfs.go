// Copyright for portions of this fork are held by [Juan Batiz-Benet, 2016]
// as part of the original go-datastore project. All other copyright for this
// fork are held by [DAOT Labs, 2020]. All rights reserved. Use of this source
// code is governed by MIT license that can be found in the LICENSE file.

package fuzzer

import (
	ds "github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	prov "github.com/daotl/go-ds-flatfs"
)

func init() {
	AddOpener("go-ds-flatfs", func(loc string) ds.Datastore {
		d, err := prov.CreateOrOpen(loc, key.KeyTypeString, prov.IPFS_DEF_SHARD, false)
		if err != nil {
			panic("could not create db instance")
		}
		return d
	})
}

package dsextensions

import (
	"context"

	"github.com/daotl/go-datastore"
	"github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
)

type QueryExt struct {
	query.Query
	SeekPrefix key.Key
}

type TxnExt interface {
	datastore.Txn
	QueryExtensions
}

type DatastoreExtensions interface {
	NewTransactionExtended(ctx context.Context, readOnly bool) (TxnExt, error)
	QueryExtensions
}

type QueryExtensions interface {
	QueryExtended(ctx context.Context, q QueryExt) (query.Results, error)
}

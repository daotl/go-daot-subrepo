package local

import (
	"time"

	"github.com/daotl/go-doubl/model"
	_ "github.com/gogo/protobuf/gogoproto"
	_ "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/daotl/go-acei/types"
)

type RequestNativeInitLedger struct {
	Time          time.Time
	LedgerId      model.LedgerID
	AppStateBytes []byte
	InitialHeight uint64
	Extra         model.ExtraPtr
}

func (m *RequestNativeInitLedger) Reset() { *m = RequestNativeInitLedger{} }

func (m *RequestNativeInitLedger) GetTime() time.Time {
	if m != nil {
		return m.Time
	}
	return time.Time{}
}

func (m *RequestNativeInitLedger) GetLedgerId() model.LedgerID {
	if m != nil {
		return m.LedgerId
	}
	return nil
}

func (m *RequestNativeInitLedger) GetAppStateBytes() []byte {
	if m != nil {
		return m.AppStateBytes
	}
	return nil
}

func (m *RequestNativeInitLedger) GetInitialHeight() uint64 {
	if m != nil {
		return m.InitialHeight
	}
	return 0
}

func (m *RequestNativeInitLedger) GetExtra() model.ExtraPtr {
	if m != nil {
		return m.Extra
	}
	return nil
}

type RequestNativeBeginBlock struct {
	Header *model.BlockHeaderExt
	Extra  model.ExtraPtr
}

func (m *RequestNativeBeginBlock) Reset() { *m = RequestNativeBeginBlock{} }

func (m *RequestNativeBeginBlock) GetHash() model.BlockHash {
	if m != nil && m.Header != nil {
		return m.Header.Hash
	}
	return nil
}

func (m *RequestNativeBeginBlock) GetHeader() *model.BlockHeaderExt {
	if m != nil {
		return m.Header
	}
	return &model.BlockHeaderExt{}
}

func (m *RequestNativeBeginBlock) GetExtra() model.ExtraPtr {
	if m != nil {
		return m.Extra
	}
	return nil
}

type RequestNativeCheckTx struct {
	Tx   *model.TransactionExt
	Type types.CheckTxType
}

func (m *RequestNativeCheckTx) Reset() { *m = RequestNativeCheckTx{} }

func (m *RequestNativeCheckTx) GetTx() *model.TransactionExt {
	if m != nil {
		return m.Tx
	}
	return nil
}

func (m *RequestNativeCheckTx) GetType() types.CheckTxType {
	if m != nil {
		return m.Type
	}
	return types.CheckTxType_New
}

type RequestNativeDeliverTx struct {
	Tx *model.TransactionExt
}

func (m *RequestNativeDeliverTx) Reset() { *m = RequestNativeDeliverTx{} }

func (m *RequestNativeDeliverTx) GetTx() *model.TransactionExt {
	if m != nil {
		return m.Tx
	}
	return nil
}

type ResponseNativeInitLedger struct {
	AppHash []byte
	Extra   model.ExtraPtr
}

func (m *ResponseNativeInitLedger) Reset() { *m = ResponseNativeInitLedger{} }

func (m *ResponseNativeInitLedger) GetAppHash() []byte {
	if m != nil {
		return m.AppHash
	}
	return nil
}

func (m *ResponseNativeInitLedger) GetExtra() model.ExtraPtr {
	if m != nil {
		return m.Extra
	}
	return nil
}

type ResponseNativeCheckTx struct {
	Code      uint32
	Data      []byte
	Log       string
	Info      string
	GasWanted int64
	GasUsed   int64
	Events    []types.Event
	Codespace string
	Sender    model.Address
	Priority  int64
	// mempool_error is set by the SMRE.
	// ACEI applications creating a ResponseCheckTX should not set mempool_error.
	MempoolError error
}

func (m *ResponseNativeCheckTx) Reset() { *m = ResponseNativeCheckTx{} }

func (m *ResponseNativeCheckTx) GetCode() uint32 {
	if m != nil {
		return m.Code
	}
	return 0
}

func (m *ResponseNativeCheckTx) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *ResponseNativeCheckTx) GetLog() string {
	if m != nil {
		return m.Log
	}
	return ""
}

func (m *ResponseNativeCheckTx) GetInfo() string {
	if m != nil {
		return m.Info
	}
	return ""
}

func (m *ResponseNativeCheckTx) GetGasWanted() int64 {
	if m != nil {
		return m.GasWanted
	}
	return 0
}

func (m *ResponseNativeCheckTx) GetGasUsed() int64 {
	if m != nil {
		return m.GasUsed
	}
	return 0
}

func (m *ResponseNativeCheckTx) GetEvents() []types.Event {
	if m != nil {
		return m.Events
	}
	return nil
}

func (m *ResponseNativeCheckTx) GetCodespace() string {
	if m != nil {
		return m.Codespace
	}
	return ""
}

func (m *ResponseNativeCheckTx) GetSender() model.Address {
	if m != nil {
		return m.Sender
	}
	return nil
}

func (m *ResponseNativeCheckTx) GetPriority() int64 {
	if m != nil {
		return m.Priority
	}
	return 0
}

func (m *ResponseNativeCheckTx) GetMempoolError() error {
	if m != nil {
		return m.MempoolError
	}
	return nil
}

type ResponseNativeEndBlock struct {
	Events []types.Event
	Extra  model.ExtraPtr
}

func (m *ResponseNativeEndBlock) Reset() { *m = ResponseNativeEndBlock{} }

func (m *ResponseNativeEndBlock) GetEvents() []types.Event {
	if m != nil {
		return m.Events
	}
	return nil
}

func (m *ResponseNativeEndBlock) GetExtra() model.ExtraPtr {
	if m != nil {
		return m.Extra
	}
	return nil
}

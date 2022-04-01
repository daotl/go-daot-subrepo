// Package cbor_refmt provides a `Marsha` implementation for CBOR backed by go-ipld-cbor and
// refmt packages.
package cbor_refmt

import (
	"io"
	"sync"

	"github.com/daotl/go-marsha"
	"github.com/daotl/go-marsha/internal/refmt"
)

// Marsha is a marsha.Marsha implementation for CBOR backed by `go-ipld-cbor` and `refmt` packages.
//
// A Struct must first be registered by calling Marsha.Register(Struct{}) before being able to be
// marshaled/unmarshaled.
//
// Marshaling/unmarshaling can be customized by `refmt` tags:
//
//   type Model struct {
//   	Foo string `refmt:"bar,omitempty"`
//   }
//
type Marsha struct {
	refmt *refmt.Refmt
}

var _ marsha.Marsha = (*Marsha)(nil)

// New creates a Marsha.
func New() *Marsha {
	return &Marsha{
		refmt: refmt.New(),
	}
}

// Register a Struct type by passing empty a Struct.
func (m *Marsha) Register(i interface{}) {
	m.refmt.RegisterCborType(i)
}

func (m *Marsha) MarshalPrimitive(p interface{}) ([]byte, error) {
	return m.refmt.Marshaller.Marshal(p)
}

func (m *Marsha) MarshalStruct(p marsha.StructPtr) ([]byte, error) {
	return m.refmt.Marshaller.Marshal(p)
}

func (m *Marsha) MarshalStructSlice(p marsha.StructSlicePtr) ([]byte, error) {
	return m.refmt.Marshaller.Marshal(p)
}

// This implementation does not support returning the count of bytes read.
func (m *Marsha) UnmarshalStruct(bin []byte, p marsha.StructPtr) (int, error) {
	return -1, m.refmt.Unmarshaller.Unmarshal(bin, p)
}

// This implementation does not support returning the count of bytes read.
func (m *Marsha) UnmarshalPrimitive(bin []byte, p interface{}) (int, error) {
	return -1, m.refmt.Unmarshaller.Unmarshal(bin, p)
}

// This implementation does not support returning the count of bytes read.
func (m *Marsha) UnmarshalStructSlice(bin []byte, p marsha.StructSlicePtr) (int, error) {
	return -1, m.refmt.Unmarshaller.Unmarshal(bin, p)
}

func (m *Marsha) NewEncoder(w io.Writer) marsha.Encoder {
	return &encoder{
		refmt: m.refmt,
		w:     w,
	}
}

func (m *Marsha) NewDecoder(r io.Reader) marsha.Decoder {
	return &decoder{
		refmt: m.refmt,
		r:     r,
	}
}

type encoder struct {
	sync.Mutex // each item must be sent atomically
	refmt      *refmt.Refmt
	w          io.Writer
}

// This implementation does not support returning the count of bytes written.
func (e *encoder) EncodePrimitive(p interface{}) (int, error) {
	return -1, e.encode(p)
}

// This implementation does not support returning the count of bytes written.
func (e *encoder) EncodeStruct(p marsha.StructPtr) (int, error) {
	return -1, e.encode(p)
}

// This implementation does not support returning the count of bytes written.
func (e *encoder) EncodeStructSlice(p marsha.StructSlicePtr) (int, error) {
	return -1, e.encode(p)
}

func (e *encoder) encode(p interface{}) error {
	// Make sure we're single-threaded through here, so multiple
	// goroutines can share an encoder.
	e.Lock()
	defer e.Unlock()
	return e.refmt.Marshaller.Encode(p, e.w)
}

type decoder struct {
	sync.Mutex // each item must be sent atomically
	refmt      *refmt.Refmt
	r          io.Reader
}

// This implementation does not support returning the count of bytes read.
func (d *decoder) DecodePrimitive(p interface{}) (int, error) {
	return -1, d.decode(p)
}

// This implementation does not support returning the count of bytes read.
func (d *decoder) DecodeStruct(p marsha.StructPtr) (int, error) {
	return -1, d.decode(p)
}

// This implementation does not support returning the count of bytes read.
func (d *decoder) DecodeStructSlice(p marsha.StructSlicePtr) (int, error) {
	return -1, d.decode(p)
}

func (d *decoder) decode(p interface{}) error {
	// Make sure we're single-threaded through here, so multiple
	// goroutines can share a decoder.
	d.Lock()
	defer d.Unlock()
	return d.refmt.Unmarshaller.Decode(d.r, p)
}

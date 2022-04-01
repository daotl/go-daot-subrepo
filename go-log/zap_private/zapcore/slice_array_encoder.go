// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zapcore

import (
	"time"

	"go.uber.org/zap/zapcore"
)

// https://github.com/uber-go/zap/blob/7b21229fb3f063275f4f169f8a79ad30aa001c51/zapcore/memory_encoder.go#L135
// SliceArrayEncoder is an ArrayEncoder backed by a simple []interface{}. Like
// the MapObjectEncoder, it's not designed for production use.
type SliceArrayEncoder struct {
	Elems []interface{}
}

func (s *SliceArrayEncoder) AppendArray(v zapcore.ArrayMarshaler) error {
	enc := &SliceArrayEncoder{}
	err := v.MarshalLogArray(enc)
	s.Elems = append(s.Elems, enc.Elems)
	return err
}

func (s *SliceArrayEncoder) AppendObject(v zapcore.ObjectMarshaler) error {
	m := zapcore.NewMapObjectEncoder()
	err := v.MarshalLogObject(m)
	s.Elems = append(s.Elems, m.Fields)
	return err
}

func (s *SliceArrayEncoder) AppendReflected(v interface{}) error {
	s.Elems = append(s.Elems, v)
	return nil
}

func (s *SliceArrayEncoder) AppendBool(v bool)              { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendByteString(v []byte)      { s.Elems = append(s.Elems, string(v)) }
func (s *SliceArrayEncoder) AppendComplex128(v complex128)  { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendComplex64(v complex64)    { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendDuration(v time.Duration) { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendFloat64(v float64)        { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendFloat32(v float32)        { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendInt(v int)                { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendInt64(v int64)            { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendInt32(v int32)            { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendInt16(v int16)            { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendInt8(v int8)              { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendString(v string)          { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendTime(v time.Time)         { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendUint(v uint)              { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendUint64(v uint64)          { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendUint32(v uint32)          { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendUint16(v uint16)          { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendUint8(v uint8)            { s.Elems = append(s.Elems, v) }
func (s *SliceArrayEncoder) AppendUintptr(v uintptr)        { s.Elems = append(s.Elems, v) }

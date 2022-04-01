// Copyright for portions of this fork are held by [Uber Technologies, 2016] as
// part of the original zap project. All other copyright for this fork are held
// by [DAOT Labs, 2020]. All rights reserved.
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

// Modified from: https://github.com/uber-go/zap/blob/7b21229fb3f063275f4f169f8a79ad30aa001c51/zapcore/console_encoder.go
package zap

import (
	"fmt"
	"sync"
	_ "unsafe"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"

	"github.com/daotl/go-log/v2/zap_private/_internal/bufferpool"
	pzapcore "github.com/daotl/go-log/v2/zap_private/zapcore"
)

var _sliceEncoderPool = sync.Pool{
	New: func() interface{} {
		return &pzapcore.SliceArrayEncoder{Elems: make([]interface{}, 0, 2)}
	},
}

func getSliceEncoder() *pzapcore.SliceArrayEncoder {
	return _sliceEncoderPool.Get().(*pzapcore.SliceArrayEncoder)
}

func putSliceEncoder(e *pzapcore.SliceArrayEncoder) {
	e.Elems = e.Elems[:0]
	_sliceEncoderPool.Put(e)
}

type compactEncoder struct {
	*pzapcore.JsonEncoder
}

// NewCompactEncoder creates an encoder whose output is designed for human -
// rather than machine - consumption. It serializes the core log entry data
// (message, level, timestamp, etc.) in a compact plain-text format and leaves
// the structured context as JSON. It's outputs look like:
//
// D[2021-05-13T17:49:52.413+0800]	example/log.go:52	for	Debug
// I[2021-05-13T17:49:52.413+0800]	example/log.go:52	for	Info
// W[2021-05-13T17:49:52.413+0800]	example/log.go:52	for	Warn
// E[2021-05-13T17:49:52.413+0800]	example/log.go:52	for	Error
// p[2021-05-13T17:49:52.413+0800]	example/log.go:52	for	DPanic
// P[2021-05-13T17:49:52.413+0800]	example/log.go:52	for	Panic
// F[2021-05-13T17:49:52.413+0800]	example/log.go:52	for	Fatal
//
// Note that although the compact encoder doesn't use the keys specified in the
// encoder configuration, it will omit any element whose key is set to the empty
// string.
func NewCompactEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder {
	if cfg.ConsoleSeparator == "" {
		// Use a default delimiter of '\t' for backwards compatibility
		cfg.ConsoleSeparator = "\t"
	}
	return compactEncoder{pzapcore.NewJSONEncoder(cfg, true)}
}

func (c compactEncoder) Clone() zapcore.Encoder {
	return compactEncoder{c.JsonEncoder.Clone().(*pzapcore.JsonEncoder)}
}

func (c compactEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	line := bufferpool.Get()

	// We don't want the entry's metadata to be quoted and escaped (if it's
	// encoded as strings), which means that we can't use the JSON encoder. The
	// simplest option is to use the memory encoder and fmt.Fprint.
	//
	// If this ever becomes a performance bottleneck, we can implement
	// ArrayEncoder for our plain-text format.
	arr := getSliceEncoder()
	if c.LevelKey != "" && c.EncodeLevel != nil {
		c.EncodeLevel(ent.Level, arr)
	}
	if c.TimeKey != "" && c.EncodeTime != nil {
		c.EncodeTime(ent.Time, arr)
	}
	if ent.LoggerName != "" && c.NameKey != "" {
		nameEncoder := c.EncodeName

		if nameEncoder == nil {
			// Fall back to FullNameEncoder for backward compatibility.
			nameEncoder = zapcore.FullNameEncoder
		}

		nameEncoder(ent.LoggerName, arr)
	}
	if ent.Caller.Defined {
		if c.CallerKey != "" && c.EncodeCaller != nil {
			c.EncodeCaller(ent.Caller, arr)
		}
		if c.FunctionKey != "" {
			arr.AppendString(ent.Caller.Function)
		}
	}
	for i := range arr.Elems {
		if i == 1 {
			line.AppendByte('[')
		} else if i == 2 {
			line.AppendByte(']')
			line.AppendString(c.ConsoleSeparator)
		} else if i > 2 {
			line.AppendString(c.ConsoleSeparator)
		}
		fmt.Fprint(line, arr.Elems[i])
	}
	putSliceEncoder(arr)

	// Add the message itself.
	if c.MessageKey != "" {
		c.addSeparatorIfNecessary(line)
		line.AppendString(ent.Message)
	}

	// Add any structured context.
	c.writeContext(line, fields)

	// If there's no stacktrace key, honor that; this allows users to force
	// single-line output.
	if ent.Stack != "" && c.StacktraceKey != "" {
		line.AppendByte('\n')
		line.AppendString(ent.Stack)
	}

	if c.LineEnding != "" {
		line.AppendString(c.LineEnding)
	} else {
		line.AppendString(zapcore.DefaultLineEnding)
	}
	return line, nil
}

func (c compactEncoder) writeContext(line *buffer.Buffer, extra []zapcore.Field) {
	context := c.JsonEncoder.Clone().(*pzapcore.JsonEncoder)
	defer func() {
		// putJSONEncoder assumes the buffer is still used, but we write out the buffer so
		// we can free it.
		context.Buf.Free()
		pzapcore.PutJSONEncoder(context)
	}()

	pzapcore.AddFields(context, extra)
	context.CloseOpenNamespaces()
	if context.Buf.Len() == 0 {
		return
	}

	c.addSeparatorIfNecessary(line)
	line.AppendByte('{')
	line.Write(context.Buf.Bytes())
	line.AppendByte('}')
}

func (c compactEncoder) addSeparatorIfNecessary(line *buffer.Buffer) {
	if line.Len() > 0 {
		line.AppendString(c.ConsoleSeparator)
	}
}

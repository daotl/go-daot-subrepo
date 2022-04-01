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

// https://github.com/uber-go/zap/blob/7b21229fb3f063275f4f169f8a79ad30aa001c51/zapcore/level_strings.go
package zapcore

import (
	"go.uber.org/zap/zapcore"

	"github.com/daotl/go-log/v2/zap_private/_internal/color"
)

var (
	_levelToColor = map[zapcore.Level]color.Color{
		zapcore.DebugLevel:  color.Magenta,
		zapcore.InfoLevel:   color.Blue,
		zapcore.WarnLevel:   color.Yellow,
		zapcore.ErrorLevel:  color.Red,
		zapcore.DPanicLevel: color.Red,
		zapcore.PanicLevel:  color.Red,
		zapcore.FatalLevel:  color.Red,
	}
	UnknownLevelColor = color.Red

	LevelToCompactString      = make(map[zapcore.Level]string, len(_levelToColor))
	LevelToCompactColorString = make(map[zapcore.Level]string, len(_levelToColor))
)

func init() {
	for level, color := range _levelToColor {
		LevelToCompactString[level] = level.CapitalString()[:1]
		LevelToCompactColorString[level] = color.Add(level.CapitalString()[:1])
	}
	LevelToCompactString[zapcore.DPanicLevel] = "p"
	LevelToCompactColorString[zapcore.DPanicLevel] = _levelToColor[zapcore.DPanicLevel].Add("p")
}

package zap

import (
	pzapcore "github.com/daotl/go-log/v2/zap_private/zapcore"
	"go.uber.org/zap/zapcore"
)

// CompactLevelEncoder serializes a Level to a compact string. For example,
// InfoLevel is serialized to "I".
func CompactLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	s, ok := pzapcore.LevelToCompactString[l]
	if !ok {
		s = l.CapitalString()[:1]
	}
	enc.AppendString(s)
}

// CompactColorLevelEncoder serializes a Level to a compact string and adds coloring.
// For example, InfoLevel is serialized to "I" and colored blue.
func CompactColorLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	s, ok := pzapcore.LevelToCompactColorString[l]
	if !ok {
		s = pzapcore.UnknownLevelColor.Add(l.CapitalString()[:1])
	}
	enc.AppendString(s)
}

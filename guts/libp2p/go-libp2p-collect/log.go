package collect

type standardLogger struct {
	Logger
}

func (l *standardLogger) funcCall(level, funcname string, args map[string]interface{}) {
	l.Logger.Logf(level, "call %s with %v", funcname, args)
}

func (l *standardLogger) message(level, msg string) {
	l.Logger.Logf(level, "message: %s", msg)
}

func (l *standardLogger) invalidArgument(name string, value interface{}) {
	l.Logger.Logf("error", "invalid argument: %s %v", name, value)
}

func (l *standardLogger) error(err error) {
	l.Logger.Logf("error", "error: %v", err)
}

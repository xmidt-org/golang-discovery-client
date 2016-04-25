package service

// Logger is the interface used for discovery logging.
// It is type compatible with several logging frameworks.
type Logger interface {
	Debug(values ...interface{})
	Warn(values ...interface{})
	Info(values ...interface{})
	Error(values ...interface{})
}

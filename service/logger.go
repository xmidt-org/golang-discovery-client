package service

import (
	"fmt"
	"io"
	"os"
)

// Logger is the interface used for discovery logging.
// It is type compatible with several logging frameworks.
type Logger interface {
	Debug(values ...interface{})
	Warn(values ...interface{})
	Info(values ...interface{})
	Error(values ...interface{})
}

// DefaultLogger provides a simple Logger implementation based
// around io.Writer
type DefaultLogger struct {
	io.Writer
}

func (logger *DefaultLogger) write(level string, values ...interface{}) {
	_, err := io.WriteString(
		logger,
		fmt.Sprintf(
			fmt.Sprintf("[%-5.5s] "+values[0].(string)+"\n", level),
			values[1:]...,
		),
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write log level %s: %v\n", level, values)
	}
}

func (logger *DefaultLogger) Debug(values ...interface{}) {
	logger.write("DEBUG", values...)
}

func (logger *DefaultLogger) Warn(values ...interface{}) {
	logger.write("WARN", values...)
}

func (logger *DefaultLogger) Info(values ...interface{}) {
	logger.write("INFO", values...)
}

func (logger *DefaultLogger) Error(values ...interface{}) {
	logger.write("ERROR", values...)
}

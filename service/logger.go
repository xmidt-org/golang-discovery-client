package service

import (
	"bytes"
	"fmt"
	"io"
)

// Logger is the interface used for discovery logging.
// It is type compatible with several logging frameworks.
type Logger interface {
	Debug(parameters ...interface{})
	Warn(parameters ...interface{})
	Info(parameters ...interface{})
	Error(parameters ...interface{})
}

// DefaultLogger provides a simple Logger implementation based
// around io.Writer
type DefaultLogger struct {
	io.Writer
}

// doWrite mimics the behavior of most logging frameworks, albeit with a simpler implementation.
func (logger DefaultLogger) doWrite(level string, parameters ...interface{}) {
	var output bytes.Buffer
	if _, err := fmt.Fprintf(&output, "[%-5.5s] ", level); err != nil {
		panic(err)
	}

	if len(parameters) > 0 {
		switch head := parameters[0].(type) {
		case fmt.Stringer:
			if _, err := fmt.Fprintf(&output, head.String(), parameters[1:]...); err != nil {
				panic(err)
			}

		case string:
			if _, err := fmt.Fprintf(&output, head, parameters[1:]...); err != nil {
				panic(err)
			}

		default:
			panic(parameters[0])
		}
	}

	if _, err := fmt.Fprintln(logger, output.String()); err != nil {
		panic(err)
	}
}

func (logger *DefaultLogger) Debug(parameters ...interface{}) {
	logger.doWrite("DEBUG", parameters...)
}

func (logger *DefaultLogger) Warn(parameters ...interface{}) {
	logger.doWrite("WARN ", parameters...)
}

func (logger *DefaultLogger) Info(parameters ...interface{}) {
	logger.doWrite("INFO ", parameters...)
}

func (logger *DefaultLogger) Error(parameters ...interface{}) {
	logger.doWrite("ERROR", parameters...)
}

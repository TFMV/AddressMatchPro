package main

import (
	"fmt"
	"log"
	"os"
)

// Logger represents a logger instance.
type Logger struct {
	*log.Logger
}

// NewLogger creates a new logger instance.
func NewLogger(prefix string) *Logger {
	return &Logger{
		Logger: log.New(os.Stdout, prefix, log.LstdFlags),
	}
}

// Info logs an informational message.
func (l *Logger) Info(format string, v ...interface{}) {
	l.Printf(fmt.Sprintf("[INFO] %s", format), v...)
}

// Debug logs a debug message.
func (l *Logger) Debug(format string, v ...interface{}) {
	l.Printf(fmt.Sprintf("[DEBUG] %s", format), v...)
}

// Error logs an error message.
func (l *Logger) Error(format string, v ...interface{}) {
	l.Printf(fmt.Sprintf("[ERROR] %s", format), v...)
}

// Fatal logs a fatal error message and exits the program.
func (l *Logger) Fatal(format string, v ...interface{}) {
	l.Printf(fmt.Sprintf("[FATAL] %s", format), v...)
	os.Exit(1)
}

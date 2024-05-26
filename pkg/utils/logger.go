// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

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

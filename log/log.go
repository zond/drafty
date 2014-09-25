package log

import (
	"fmt"
	goLog "log"
	"time"
)

var Level = 0

const (
	Debug = iota
	Info
	Warn
	Error
	Fatal
)

func log(f func(string, ...interface{}), prefix string, format string, args ...interface{}) {
	format = fmt.Sprintf("%v\t%v\t%v", prefix, time.Now(), format)
	f(format, args...)
}

func Debugf(format string, args ...interface{}) {
	if Level <= Debug {
		log(goLog.Printf, "DEBUG", format, args...)
	}
}

func Infof(format string, args ...interface{}) {
	if Level <= Info {
		log(goLog.Printf, "INFO", format, args...)
	}
}

func Warnf(format string, args ...interface{}) {
	if Level <= Warn {
		log(goLog.Printf, "WARN", format, args...)
	}
}

func Errorf(format string, args ...interface{}) {
	if Level <= Error {
		log(goLog.Printf, "ERROR", format, args...)
	}
}

func Fatalf(format string, args ...interface{}) {
	if Level <= Fatal {
		log(goLog.Panicf, "FATAL", format, args...)
	}
}

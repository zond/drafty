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

func log(prefix string, format string, args ...interface{}) {
	format = fmt.Sprintf("%v\t%v\t%v", prefix, time.Now(), format)
	goLog.Printf(format, args...)
}

func Debugf(format string, args ...interface{}) {
	if Level <= Debug {
		log("DEBUG", format, args...)
	}
}

func Infof(format string, args ...interface{}) {
	if Level <= Info {
		log("INFO", format, args...)
	}
}

func Warnf(format string, args ...interface{}) {
	if Level <= Warn {
		log("WARN", format, args...)
	}
}

func Errorf(format string, args ...interface{}) {
	if Level <= Error {
		log("ERROR", format, args...)
	}
}

func Fatalf(format string, args ...interface{}) {
	if Level <= Fatal {
		log("FATAL", format, args...)
	}
}

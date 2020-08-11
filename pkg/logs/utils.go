package logs

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

// formatter adds default fields to each log entry.
type formatter struct {
	owner string
	lf    log.Formatter
}

// Format satisfies the log.Formatter interface.
func (f *formatter) Format(e *log.Entry) ([]byte, error) {
	e.Message = fmt.Sprintf("[%s] %s", f.owner, e.Message)
	return f.lf.Format(e)
}

func NewLogger(owner string) *log.Logger {
	logger := log.New()
	logger.SetFormatter(&formatter{
		owner: owner,
		lf: &log.TextFormatter{
			ForceColors:     true,
			FullTimestamp:   true,
			TimestampFormat: time.StampMilli,
		},
	})
	return logger
}

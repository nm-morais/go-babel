package errors

import log "github.com/sirupsen/logrus"

type Error interface {
	Fatal() bool
	Temporary() bool
	Code() int
	Reason() string
	Caller() string
	Log()
}

func NonFatalError(code int, reason string, caller string) Error {
	return &genericErr{
		fatal:     true,
		temporary: false,
		code:      code,
		reason:    reason,
		caller:    caller,
	}
}

func FatalError(code int, reason string, caller string) Error {
	return &genericErr{
		fatal:     true,
		temporary: false,
		code:      code,
		reason:    reason,
		caller:    caller,
	}
}

func TemporaryError(code int, reason string, caller string) Error {
	return &genericErr{
		fatal:     false,
		temporary: true,
		code:      code,
		reason:    reason,
		caller:    caller,
	}
}

type genericErr struct {
	fatal     bool
	temporary bool
	code      int
	reason    string
	caller    string
}

func (err *genericErr) Log() {
	log.Errorf("[%s]: Error type: %d, Reason: %s", err.Caller(), err.Code(), err.Reason())
}

func (err *genericErr) Fatal() bool {
	return err.fatal
}

func (err *genericErr) Temporary() bool {
	return err.temporary
}

func (err *genericErr) Code() int {
	return err.code
}

func (err *genericErr) Caller() string {
	return err.caller
}

func (err *genericErr) Reason() string {
	return err.reason
}

package utils

import . "github.com/DeMMon/go-babel/pkg"

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
	return err.Caller()
}

func (err *genericErr) Reason() string {
	return err.Caller()
}

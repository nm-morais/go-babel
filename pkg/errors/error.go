package errors

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

type Error interface {
	Fatal() bool
	Code() int
	Reason() string
	Caller() string
	ToString() string
	Wrap(otherError Error, caller string) Error
	Log(logger *logrus.Logger)
}

func NonFatalError(code int, reason string, caller string) Error {
	return &genericErr{
		fatal:  true,
		code:   code,
		reason: reason,
		caller: caller,
	}
}

func FatalError(code int, reason string, caller string) Error {
	return &genericErr{
		fatal:  true,
		code:   code,
		reason: reason,
		caller: caller,
	}
}

func TemporaryError(code int, reason string, caller string) Error {
	return &genericErr{
		fatal:  false,
		code:   code,
		reason: reason,
		caller: caller,
	}
}

type genericErr struct {
	fatal  bool
	code   int
	reason string
	caller string
}

func (err *genericErr) Log(logger *logrus.Logger) {
	if err.fatal {
		logger.Fatal(err.ToString())
	}
	logger.Error(err.ToString())
}

func (err *genericErr) Wrap(otherError Error, caller string) Error {
	return &genericErr{
		fatal:  otherError.Fatal(),
		code:   otherError.Code(),
		reason: fmt.Sprintf("Got error from [%s]: %s", otherError.ToString()),
		caller: caller,
	}
}

func (err *genericErr) ToString() string {
	return fmt.Sprintf("Error type: %d, Reason: %s", err.Code(), err.Reason())
}

func (err *genericErr) Fatal() bool {
	return err.fatal
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

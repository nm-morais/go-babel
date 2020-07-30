package pkg

type ID = string
type MessageType = string

type Error interface {
	Fatal() bool
	Temporary() bool
	Code() int
	Reason() string
	Caller() string
}

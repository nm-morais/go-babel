package pkg

type MessageType = uint64

type Error interface {
	Fatal() bool
	Temporary() bool
	Code() int
	Reason() string
	Caller() string
}

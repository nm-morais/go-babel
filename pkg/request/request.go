package request

type ID = uint16

type Request interface {
	ID() ID
}

type Reply interface {
	ID() ID
}

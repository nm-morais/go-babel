package notification

type ID uint16

type Notification interface {
	ID() ID
}

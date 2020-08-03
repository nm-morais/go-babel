package timer

type ID uint16

type Timer interface {
	ID() ID
	Wait()
}

package timer

import "time"

type ID = uint16

type Timer interface {
	ID() ID
	Duration() time.Duration
}

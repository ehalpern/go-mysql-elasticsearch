package river

import (
	"github.com/siddontang/go/sync2"
	"net"
)

type statistics struct {
	r *River

	l net.Listener

	InsertNum sync2.AtomicInt64
	UpdateNum sync2.AtomicInt64
	DeleteNum sync2.AtomicInt64
}

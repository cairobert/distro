package distro

type Mutex struct {
	lkCh chan int
}

func NewMutex() *Mutex {
	return &Mutex{make(chan int, 1)}
}

func (m *Mutex) Lock() {
	m.lkCh <- 0
}

func (m *Mutex) Unlock() {
	<- m.lkCh
}

package mongonet

import "fmt"
import "net"
import "testing"
import "time"

type FakeServer struct {
	numAccepted int
}

func (fs *FakeServer) doThread(conn net.Conn, threadNumber int) {
	defer conn.Close()
	fmt.Printf("connection from %s\n", conn.RemoteAddr())
}

func (fs *FakeServer) run(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		fs.numAccepted++
		go fs.doThread(conn, fs.numAccepted)
	}
}

func (fs *FakeServer) start(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	go fs.run(ln)
	return nil
}

func fun(cp *ConnectionPool) error {
	conn, err := cp.Get()
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}

func funBad(cp *ConnectionPool) error {
	conn, err := cp.Get()
	if err != nil {
		return err
	}
	conn.bad = true
	defer conn.Close()
	return nil
}

func TestConnectionPool1(t *testing.T) {
	port := 12349
	fs := FakeServer{}
	err := fs.start(port)
	if err != nil {
		t.Errorf("can't start %s", err)
	}

	cp := NewConnectionPool(fmt.Sprintf("127.0.0.1:%d", port), false, nil, false, nil)
	cp.timeoutSeconds = 1

	// first loop
	err = fun(cp)
	if err != nil {
		t.Errorf("error funning %s", err)
		return
	}

	if cp.LoadTotalCreated() != 1 {
		t.Errorf("why is total created %d", cp.LoadTotalCreated())
		return
	}

	// 2nd loop, should re-use connection
	err = fun(cp)
	if err != nil {
		t.Errorf("error funning %s", err)
		return
	}

	if cp.LoadTotalCreated() != 1 {
		t.Errorf("why is total created %d", cp.LoadTotalCreated())
		return
	}

	garbage, err := cp.Get()
	if err != nil {
		panic(err)
	}
	defer garbage.Close()

	err = fun(cp)
	if err != nil {
		t.Errorf("error funning %s", err)
		return
	}

	if cp.LoadTotalCreated() != 2 {
		t.Errorf("why is total created %d", cp.LoadTotalCreated())
		return
	}

	time.Sleep(time.Duration(int64(time.Second) * (cp.timeoutSeconds + 1)))

	err = fun(cp)
	if err != nil {
		t.Errorf("error funning %s", err)
		return
	}

	if cp.LoadTotalCreated() != 3 {
		t.Errorf("why is total created %d", cp.LoadTotalCreated())
		return
	}

	before := cp.CurrentInPool()

	err = funBad(cp)
	if err != nil {
		panic(err)
	}

	after := cp.CurrentInPool()
	if after != before-1 {
		t.Errorf("bad didn't work %d -> %d", before, after)
		return
	}

}

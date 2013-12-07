package recon

import (
	"io/ioutil"
	"net"
	"os"
	"testing"

	"github.com/bmizerany/assert"

	. "github.com/cmars/conflux"
)

func createMemPeer() *Peer {
	peer := NewMemPeer()
	go peer.handleCmds()
	return peer
}

func recoverPeer(t *testing.T, peer *Peer) {
	for {
		select {
		case r, ok := <-peer.RecoverChan:
			if !ok {
				return
			}
			for _, zp := range r.RemoteElements {
				t.Log("Recover", zp)
				peer.Insert(zp)
			}
		}
	}
}

func TestRecon(t *testing.T) {
	var sock string
	{
		f, err := ioutil.TempFile("", "sock")
		assert.Equal(t, nil, err)
		defer f.Close()
		sock = f.Name()
	}
	err := os.Remove(sock)
	assert.Equal(t, nil, err)
	peer1 := createMemPeer()
	peer2 := createMemPeer()
	go recoverPeer(t, peer1)
	go recoverPeer(t, peer2)
	peer1.PrefixTree.Insert(Zi(P_SKS, 1))
	peer1.PrefixTree.Insert(Zi(P_SKS, 3))
	peer1.PrefixTree.Insert(Zi(P_SKS, 5))
	peer2.PrefixTree.Insert(Zi(P_SKS, 1))
	peer2.PrefixTree.Insert(Zi(P_SKS, 4))
	peer2.PrefixTree.Insert(Zi(P_SKS, 9))
	errs := make(chan error, 2)
	l, err := net.Listen("unix", "/tmp/test.sock")
	assert.Equal(t, nil, err)
	defer l.Close()
	go func() {
		c1, err := l.Accept()
		assert.Equal(t, nil, err)
		err = peer1.accept(c1)
		errs <- err
	}()
	go func() {
		c2, err := net.Dial("unix", "/tmp/test.sock")
		assert.Equal(t, nil, err)
		err = peer2.initiateRecon(c2)
		errs <- err
	}()
	err = <-errs
	assert.Equal(t, nil, err)
	err = <-errs
	assert.Equal(t, nil, err)
	t.Log("done")
}

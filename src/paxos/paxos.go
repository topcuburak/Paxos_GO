package paxos

// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type PhaseArg struct {
	Bal int
	Seq int
	Val interface{}
}

type Seqinfo struct {
	prop_val       interface{}
	responses      []PhaseArg
	round          int
	max_ballot     int
	accepted_val   interface{}
	proposer       bool
	paxos_finished bool
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	seqmap     map[int]Seqinfo
	local_min  int
	mins       []int
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {

	px.mu.Lock()
	if seq < px.local_min {
		px.mu.Unlock()
		return
	}

	_, ok := px.seqmap[seq]

	if !ok {
		seqinfo := Seqinfo{}

		seqinfo.responses = make([]PhaseArg, len(px.peers))
		seqinfo.round = 1
		seqinfo.prop_val = v
		seqinfo.max_ballot = 0
		seqinfo.accepted_val = nil
		seqinfo.proposer = true
		px.seqmap[seq] = seqinfo
	} else {
		seqinfo := px.seqmap[seq]
		seqinfo.prop_val = v
		seqinfo.proposer = true
		px.seqmap[seq] = seqinfo
	}
	px.mu.Unlock()
	go px.paxos(seq)
}

// Runs paxos algorithm for a round
func (px *Paxos) paxos(seq int) {
	// Phase 1
	px.mu.Lock()
	seqinfo := px.seqmap[seq]
	px.mu.Unlock()
	var bal int

	for true {

		for true {
			bal = seqinfo.round * int(px.me+1)

			var wg sync.WaitGroup

			for ind, peer := range px.peers {
				wg.Add(1)
				go px.phase1(peer, ind, bal, seq, &wg)
			}
			wg.Wait()
			//fmt.Println(//fmt.Sprintf("Got response from everyone, me: %d, seq: %d", px.me, seq))

			var max_bal int
			max_bal = 0
			promise_count := 0

			px.mu.Lock()
			for _, response := range px.seqmap[seq].responses {
				//fmt.Println(//fmt.Sprintf("Ind %d, response: %d", ind, response))
				if response.Seq == 1 {
					promise_count++
					if response.Bal > max_bal && response.Val != nil {
						max_bal = response.Bal
						seqinfo.prop_val = response.Val
					}
				}
			}
			px.mu.Unlock()

			//fmt.Println(//fmt.Sprintf("me: %d, seq: %d, acceptor's max_bal: %d", px.me, seq, max_bal))

			if promise_count > len(px.peers)/2 {
				//fmt.Println(//fmt.Sprintf("Majority done for seq: %d", seq))
				break
			}

			seqinfo.round++
			time.Sleep(time.Duration(rand.Intn(1000) * 1000))
		}

		//fmt.Print("Phase 1 completed, seq: ")
		//fmt.Println(seq)

		// Phase 2

		var wg sync.WaitGroup

		for ind, peer := range px.peers {
			wg.Add(1)
			go px.phase2(peer, ind, bal, seq, seqinfo.prop_val, &wg)
		}
		wg.Wait()
		//fmt.Println("Got an accept response from everyone")
		sum := 0

		px.mu.Lock()
		for _, response := range px.seqmap[seq].responses {
			sum += response.Seq
		}
		px.mu.Unlock()
		if sum > len(px.peers)/2 {
			//fmt.Println(//fmt.Sprintf("Majority accepted me: %d, seq: %d!", px.me, seq))
			newval := seqinfo.prop_val
			px.mu.Lock()
			seqinfo = px.seqmap[seq]
			seqinfo.accepted_val = newval
			seqinfo.paxos_finished = true
			px.seqmap[seq] = seqinfo
			px.mu.Unlock()
			break
		}
		seqinfo.round++
		time.Sleep(time.Duration(rand.Intn(1000) * 1000))

	}
}

// Phase 1
func (px *Paxos) phase1(peer string, ind int, bal int, seq int, wg *sync.WaitGroup) {

	var ret PhaseArg
	p1arg := PhaseArg{bal, seq, nil}

	call(peer, "Paxos.Promise", &p1arg, &ret)
	px.mu.Lock()
	px.seqmap[seq].responses[ind] = ret
	px.mu.Unlock()
	wg.Done()
}

// Phase 2
func (px *Paxos) phase2(peer string, ind int, bal int, seq int, val interface{}, wg *sync.WaitGroup) {

	var ret PhaseArg
	p2arg := PhaseArg{bal, seq, val}

	call(peer, "Paxos.Accept", &p2arg, &ret)

	px.mu.Lock()
	px.seqmap[seq].responses[ind] = ret
	px.mu.Unlock()

	wg.Done()
}

// Decides to accept or not
func (px *Paxos) Promise(arg *PhaseArg, reply *PhaseArg) error {
	if arg.Seq < px.Min() {
		return nil
	}
	px.mu.Lock()
	seqinfo, ok := px.seqmap[arg.Seq]
	if !ok {
		seqinfo := Seqinfo{}
		seqinfo.responses = make([]PhaseArg, len(px.peers))
		seqinfo.round = 1
		seqinfo.max_ballot = 0
		seqinfo.accepted_val = nil
		px.seqmap[arg.Seq] = seqinfo
	}
	px.mu.Unlock()

	//TODO: use a more granular mutex
	px.mu.Lock()
	if arg.Bal >= seqinfo.max_ballot {
		reply.Bal = seqinfo.max_ballot
		reply.Val = seqinfo.accepted_val
		reply.Seq = 1
		seqinfo.max_ballot = arg.Bal
		px.seqmap[arg.Seq] = seqinfo
		px.mu.Unlock()
		return nil
	}
	px.mu.Unlock()

	reply.Seq = 0
	return nil
}

// Decides to accept or not
func (px *Paxos) Accept(arg *PhaseArg, response *PhaseArg) error {
	if arg.Seq < px.Min() {
		return nil
	}
	px.mu.Lock()
	seqinfo, _ := px.seqmap[arg.Seq]
	if arg.Bal == seqinfo.max_ballot {
		seqinfo.accepted_val = arg.Val
		px.seqmap[arg.Seq] = seqinfo
		px.mu.Unlock()
		response.Seq = 1
		return nil
	}

	px.mu.Unlock()
	response.Seq = 0
	return nil
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.local_min = seq + 1
	args := PhaseArg{px.local_min, px.me, nil}
	var resp interface{}
	for _, val := range px.peers {
		go call(val, "Paxos.UpdateMin", &args, &resp)
	}
}

// Decides to accept or not
func (px *Paxos) UpdateMin(arg *PhaseArg, response *PhaseArg) error {
	px.mu.Lock()
	px.mins[arg.Seq] = arg.Bal
	px.mu.Unlock()
	return nil
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	if px == nil {
		return -1
	} else {
		px.mu.Lock()
		keys := make([]int, len(px.seqmap))
		i := 0
		for k := range px.seqmap {
			keys[i] = k
			i++
		}
		px.mu.Unlock()

		var max int = keys[0]
		for _, value := range keys {
			if max < value {
				max = value
			}
		}
		return max
	}
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest seq number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	min := 100000

	for _, val := range px.mins {
		if val < min {
			min = val
		}

	}
	// Delete Forgotten keys
	px.mu.Lock()
	for k := range px.seqmap {
		if k < min {
			if !px.seqmap[k].proposer {
				delete(px.seqmap, k)
				//fmt.Println(//fmt.Sprintf("me: %d, deleted seq: %d", px.me, k))
			} else if px.seqmap[k].paxos_finished {
				delete(px.seqmap, k)
				//fmt.Println(//fmt.Sprintf("me: %d, deleted seq: %d", px.me, k))
			}
		}
	}
	px.mu.Unlock()

	return min
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	//TODO: clear forgotten keys

	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	seqinfo, ok := px.seqmap[seq]
	px.mu.Unlock()
	if ok {
		if seqinfo.accepted_val != nil {
			return Decided, seqinfo.accepted_val
		}
	}

	return Pending, nil
}

// tell the peer to shut itself down.
// for testing.atomic
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	px.mins = make([]int, len(px.peers))
	for i := 0; i < len(px.peers); i++ {
		px.mins[i] = 0
	}
	px.seqmap = make(map[int]Seqinfo)

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							//fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					//fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

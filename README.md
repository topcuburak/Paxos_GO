Name 1: [Burak Topcu] \
Name 2: [Omer Faruk Ozdemir] 

## Project Overview
In this assignment, we implemented Paxos, specifically single-decree paxos.

**Project Details**

Here is the Paxos pseudo-code:
```
proposer(v):
    while not decided:
        choose n, unique and higher than any n seen so far
        send prepare(n) to all servers including self
        if prepare_ok(n, n_a, v_a) from majority:
            v' = v_a with highest n_a; choose own v otherwise
            send accept(n, v') to all
            if accept_ok(n) from majority:
                send decided(v') to all

acceptor's state:
    n_p (highest prepare seen)
    n_a, v_a (highest accept seen)

acceptor's prepare(n) handler:
    if n > n_p
        n_p = n
        reply prepare_ok(n, n_a, v_a)
    else
        reply prepare_reject

acceptor's accept(n, v) handler:
    if n >= n_p
        n_p = n
        n_a = n
        v_a = v
        reply accept_ok(n)
    else
        reply accept_reject
```

## Test
```
Test: Single proposer ...    
... Passed
Test: Many proposers, same value ...
... Passed
Test: Many proposers, different values ...
... Passed
Test: Out-of-order instances ...
... Passed
Test: Deaf proposer ...
... Passed
Test: Forgetting ...
... Passed
Test: Lots of forgetting ...
... Passed
Test: Paxos frees forgotten instance memory ...
... Passed
Test: Many instances ...
... Passed
Test: Minority proposal ignored ...
... Passed
Test: Many instances, unreliable RPC ...
... Passed
Test: No decision if partitioned ...
... Passed
Test: Decision in majority partition ...
... Passed
Test: All agree after full heal ...
... Passed
Test: One peer switches partitions ...
... Passed
Test: One peer switches partitions, unreliable ...
... Passed
Test: Many requests, changing partitions ...
... Passed
PASS
ok      paxos   59.523s
```

# Chord Distributed Lookup (MPI)

An implementation of **Chord**, the distributed hash table lookup protocol, in C with **MPI**. Each MPI process is a node on a Chord ring. Nodes route key lookups to the responsible node in **O(log N) hops** using finger tables, exchanging only point-to-point messages (`MPI_Send` / `MPI_Recv`).

All 8 tests pass, for both correctness and efficiency (the hop count must stay within `⌈log₂ N⌉ + 2`).

## How it works

The identifier space is `2^m = 16` IDs (`m = 4`). Every process reads its node ID and a list of keys to look up from `in<rank>.txt`. The nodes then form a static ring, sorted by ID, where each node knows its successor and predecessor.

### Finger table: `build_finger_table()`
For each `i` in `0 … m-1`:
- `start[i] = (id + 2^i) mod 2^m`
- `finger[i] = successor(start[i])`, searched among the existing nodes only (static ring).

### Routing: `closest_preceding_finger(key)`
- Fingers are scanned from the farthest to the nearest. The function returns the first finger that lies strictly inside the circular open interval `(self.id, key)`, i.e. the longest valid jump toward the key.
- The interval check handles both the normal case (`self.id < key`) and the wrap-around case (`self.id > key`).
- If no finger matches, it falls back to the successor. This guarantees progress and avoids routing cycles.

### Lookup handling: `handle_lookup_request(msg)`
1. The node appends itself to the message's route.
2. If `key ∈ (self.id, successor]`, the successor is responsible. The node appends it to the route and sends a `TAG_LOOKUP_REP` straight back to the initiator.
3. Otherwise, it forwards a `TAG_LOOKUP_REQ` to `closest_preceding_finger(key)`.

### Starting lookups
Each node sends its own lookups to **itself** as `TAG_LOOKUP_REQ`. That way the first hop goes through exactly the same code path as every other hop. The original keys are kept so that replies, which can arrive in any order, are matched to their requests and printed in input order.

### Distributed service loop and termination
Every node runs one loop that receives messages from any source with any tag:
- `TAG_LOOKUP_REQ`: route the request (see above).
- `TAG_LOOKUP_REP`: store the result and mark that lookup as finished.
- `TAG_DONE`: count another node that has finished.

When a node has received all of its replies, it broadcasts `TAG_DONE` once to every other node. A node exits only after its own lookups are done **and** it has received `DONE` from all `world_size - 1` peers. This way no node stops while others may still need it to forward requests.

## Example

Input files (`in<rank>.txt` holds the node ID, the number of lookups, then the keys):

```
in0.txt: 1   1  7
in1.txt: 5   1  11
in2.txt: 10  1  2
```

Output (each node prints its own lookups, so lines from different nodes may appear in any order):

```
Lookup 7: 1 -> 5 -> 10
Lookup 11: 5 -> 10 -> 1
Lookup 2: 10 -> 1 -> 5
```

## Build & run

Requires an MPI implementation (e.g. OpenMPI: `openmpi-bin`, `libopenmpi-dev`).

```bash
make build
mpirun --oversubscribe -np 3 ./tema2   # run in a directory containing in0.txt … in2.txt
```

To run the full test suite (correctness + O(log N) hop check):

```bash
cd checker
./checker.sh
```

## Repository layout

```
tema2.c                  Chord implementation
Makefile
checker/checker.sh       runs all tests with mpirun
checker/check_lookup.py  verifies the final successor and the hop count
checker/tests/           8 test scenarios (2 to 16 nodes)
```

## Credits

Developed as an assignment for the *Parallel and Distributed Algorithms* course at the Faculty of Automatic Control and Computers, Politehnica University of Bucharest (2025). The course team provided the skeleton (input parsing, ring construction, ID mapping, data structures) and the checker. The finger table, routing, lookup handling and distributed termination are my own work.

# go-rpc-system

A lightweight **Remote Procedure Call (RPC) framework in Go**, built from scratch on raw TCP sockets, JSON, and runtime reflection — no external dependencies.

Register any Go struct on a server, and clients can call its exported methods over the network as if they were local.

![Go](https://img.shields.io/badge/Go-1.20+-00ADD8?logo=go&logoColor=white)
![Tests](https://img.shields.io/badge/tests-17%20passing-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-90%25-brightgreen)
![Race](https://img.shields.io/badge/go%20test%20--race-clean-brightgreen)
![Dependencies](https://img.shields.io/badge/dependencies-0-blue)

---

## Highlights

- **Reflection-based dispatch** — any exported method on a pointer receiver becomes remotely callable; arguments are decoded into the method's real parameter types at runtime.
- **Concurrent server** — a background accept loop hands each connection to its own goroutine worker.
- **Graceful lifecycle** — `Start` / `Stop` / restart, duplicate-start protection, port `0` discovery, and `Stop` waits for in-flight calls to finish.
- **Hardened against bad input** — malformed JSON, oversized payloads, too many arguments, unsafe method names, unknown methods, and wrong argument types all return clear errors instead of crashing the server.
- **Panic isolation** — a panic inside a remote method is recovered and sent back to the client as an error; the server keeps running.
- **Deadlines everywhere** — server-side and client-side timeouts so a silent or slow peer can never hang a call or block shutdown.

## Architecture

```
 ┌──────────────┐   JSON request over TCP    ┌─────────────────────────────────────┐
 │    Client    │ ─────────────────────────▶ │               Server                │
 │              │  {"method":"Ping",         │                                     │
 │ Call(method, │   "args":[1]}              │  acceptLoop ──▶ worker goroutine    │
 │   &out,args) │                            │                   │                 │
 │              │ ◀───────────────────────── │  validateRequest ─┤                 │
 └──────────────┘   JSON response            │  invoke (reflect) ┤ recover panics  │
                    {"ok":true,              │  encode result  ◀─┘                 │
                     "value":"Pong1"}        └─────────────────────────────────────┘
```

**Protocol:** one request and one response per connection, newline-delimited JSON.

| Message  | Shape |
|----------|-------|
| Request  | `{"method": "Name", "args": [<json>, ...]}` |
| Response | `{"ok": true, "value": <json>}` or `{"ok": false, "error": "..."}` |

## Quick start

```bash
git clone <repo-url>
cd go-rpc-system
go run ./cmd/ping
```

Output:

```
Pong1
Pong2
Pong3
Pong4
```

## Usage

```go
package main

import (
	"fmt"
	"log"

	"lab1/rpc"
)

type Calculator struct{}

func (*Calculator) Add(a, b int) int { return a + b }

func (*Calculator) Divide(a, b float64) (float64, error) {
	if b == 0 {
		return 0, fmt.Errorf("division by zero")
	}
	return a / b, nil
}

func main() {
	server, err := rpc.NewServer(&Calculator{})
	if err != nil {
		log.Fatal(err)
	}
	if err := server.Start("127.0.0.1:0"); err != nil { // port 0 = pick a free port
		log.Fatal(err)
	}
	defer server.Stop()

	client, _ := rpc.NewClient(server.Address())

	var sum int
	_ = client.Call("Add", &sum, 2, 3)
	fmt.Println(sum) // 5

	var q float64
	err = client.Call("Divide", &q, 1.0, 0.0)
	fmt.Println(err) // division by zero
}
```

### Supported method shapes

| Signature | Example |
|-----------|---------|
| No args, no return | `func (*T) Reset()` |
| Typed args | `func (*T) Add(a, b int) int` |
| Value + error | `func (*T) Get(id int) (User, error)` |
| Error only | `func (*T) Save(u User) error` |

## Safety limits

| Limit | Value |
|-------|-------|
| Max arguments per call | 8 |
| Max size per argument | 1 MiB |
| Max request size | 4 MiB |
| Default call timeout | 5 s |

## Testing

```bash
go test -race -cover ./...
```

The suite (17 tests, ~90% coverage, race-detector clean) covers:

- successful calls across all method shapes
- unknown methods, wrong argument counts, invalid JSON arguments
- remote errors and remote panics
- malformed and oversized raw requests sent directly over TCP
- concurrent calls from many goroutines
- stop / restart, duplicate start, port-0 discovery, fixed addresses
- `Stop` waiting for an active handler, and a silent client not blocking `Stop`
- client timeouts and connection failures

## Project structure

```
.
├── cmd/ping/main.go   # runnable demo: Ping → Pong
├── rpc/rpc.go         # server, client, protocol, reflection bridge
├── rpc/rpc_test.go    # test suite
└── go.mod
```

## Key concepts

- TCP server design in Go: listeners, accept loops, and per-connection goroutines
- Coordinating shutdown with channels, `sync.Mutex`, and `sync.WaitGroup`
- Using `reflect` to call methods dynamically with type-safe argument decoding
- Defensive network programming: input limits, deadlines, and panic recovery
- Writing concurrency tests and running them under the race detector

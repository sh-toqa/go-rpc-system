// Package rpc is an intentionally incomplete RMI assignment template.
package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
	"unicode"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
	// json.RawMessage is a []byte that delays JSON decoding until we know the type
}

type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
type Server struct {
	receiver   any
	listener   net.Listener
	stopped    chan struct{}
	acceptDone chan struct{}
	workers    sync.WaitGroup
	mu         sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}
	value := reflect.ValueOf(receiver)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}
	return &Server{receiver: receiver}, nil
}

// TODO 1: Create the TCP listener, reject duplicate starts, save lifecycle state,
// and launch the accept loop without blocking the caller.
// Start begins accepting remote calls at address. Use port 0 to request an available port.
func (s *Server) Start(address string) error {
	//another goroutine cannot acquire the same mutex until it is unlocked
	//if another goroutine is already inside Start, Stop, or Address, this one waits here until it’s free
	s.mu.Lock() // Lock the mutex to ensure thread-safe access to the server's state

	// defer = schedule this line to run at the END of the function
	defer s.mu.Unlock() // Unlock the mutex when the function returns

	//flag: nil means stopped, not nil means running
	if s.listener != nil {
		return errors.New("server already started") //reject 2nd start
	}
	//reserve this address/port for me and deliver incoming TCP connections
	// returns (listener, nil) on success or (nil, err) on failure
	//Any TCP traffic that arrives for port X belongs to me
	//the OS marks the socket as listening
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err) // %w wraps the original error for better error handling
	}

	s.listener = listener // save it in the server  the server is now "running"
	//Stop accepting new customers
	s.stopped = make(chan struct{}) // create a fresh signal channel for this run
	//client left
	s.acceptDone = make(chan struct{}) // create a fresh loop finished channel for this run
	// go = run acceptLoop in a NEW goroutine (in the background)
	// we pass the local variables, so the loop has its own copies and never needs to read s.listener (which would need the lock)
	go s.acceptLoop(listener, s.stopped, s.acceptDone) // listener not s.listener because we want to pass the local variable, not the struct field

	return nil // success; returns immediately without waiting for any client
}

// <-chan struct{} means this function can receive from the channel.
// chan<- struct{} means this function can send to the channel or close it, provided it has the appropriate ownership
// sit at the door forever, let clients in one by one, and give each client to a worker goroutine. Exit when Stop says so.
func (s *Server) acceptLoop(listener net.Listener, stopped <-chan struct{}, done chan<- struct{}) {
	// signal that the accept loop is done when this function returns
	defer close(done)
	// The loop will keep accepting new connections until the listener is closed or an error occurs
	for {
		//wait for a new client to connect, or for the listener to be closed
		//Accept() blocks until a new connection is available or the listener is closed
		conn, err := listener.Accept() //Waits for a client connection
		if err != nil {
			select {
			case <-stopped:
				return // Stop() closed the listener on purpose
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(10 * time.Millisecond) // transient error: back off and retry
			continue
		}

		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			s.handle(conn)
		}()
	}
}

// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.
func (s *Server) Address() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return "" // before start and after stop
	}
	return s.listener.Addr().String()
}

// TODO 3: Close the listener, signal the accept loop, wait for its workers,
// and leave the server in a state where Start can be called again.
// Stop stops accepting new connections and waits for active handlers to finish.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return nil
	}

	if err := s.listener.Close(); err != nil {
		return fmt.Errorf("failed to close listener: %w", err)
	}

	close(s.stopped)
	<-s.acceptDone

	s.workers.Wait()
	s.listener = nil
	return nil
}

// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.
func (s *Server) handle(connection net.Conn) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(defaultCallTimeout))
	encoder := json.NewEncoder(connection)
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = encoder.Encode(response{Error: fmt.Sprintf("remote method panicked: %v", recovered)})
		}
	}()

	decoder := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, maxRequestBytes+1)))
	var req request
	if err := decoder.Decode(&req); err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("invalid request: %v", err)})
		return
	}
	if err := validateRequest(req); err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	result, err := s.invoke(req)
	if err != nil {
		_ = encoder.Encode(response{Error: err.Error()})
		return
	}
	payload, err := json.Marshal(result)
	if err != nil {
		_ = encoder.Encode(response{Error: fmt.Sprintf("encode result: %v", err)})
		return
	}
	_ = encoder.Encode(response{OK: true, Value: payload})
}

// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.
func validateRequest(req request) error {
	if req.Method == "" {
		return errors.New("method cannot be empty")
	}

	for _, r := range req.Method {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("method %q contains whitespace or control characters", req.Method)
		}
	}

	if len(req.Args) > maxArguments {
		return fmt.Errorf("too many arguments: got %d, maximum is %d", len(req.Args), maxArguments)
	}

	total := 0
	for i, arg := range req.Args {
		if len(arg) > maxArgumentBytes {
			return fmt.Errorf("argument %d is larger than %d bytes", i, maxArgumentBytes)
		}
		total += len(arg)
	}
	if total > maxRequestBytes {
		return fmt.Errorf("request arguments are larger than %d bytes", maxRequestBytes)
	}

	return nil
}

// TODO 6: Study and test this starter reflection bridge. Your required work is to
// support and test exported methods with zero arguments, typed arguments, no
// return value, one return value, and a final error return; unknown methods,
// wrong argument counts, invalid JSON arguments, returned errors, and panics must
// become clear errors instead of crashing the server. Extend the bridge only when
// your chosen assignment protocol requires another result or argument rule.
// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
func (s *Server) invoke(req request) (any, error) {
	method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
	if !method.IsValid() {
		return nil, fmt.Errorf("unknown method %q", req.Method)
	}
	typeOfMethod := method.Type()
	if typeOfMethod.NumIn() != len(req.Args) {
		return nil, fmt.Errorf("method %q expects %d arguments, got %d", req.Method, typeOfMethod.NumIn(), len(req.Args))
	}
	arguments := make([]reflect.Value, len(req.Args))
	for index, raw := range req.Args {
		argument := reflect.New(typeOfMethod.In(index))
		if err := json.Unmarshal(raw, argument.Interface()); err != nil {
			return nil, fmt.Errorf("argument %d: %v", index, err)
		}
		arguments[index] = argument.Elem()
	}
	outputs, err := callMethodSafely(method, arguments)
	if err != nil {
		return nil, err
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	if last := outputs[len(outputs)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		if !isNilValue(last) {
			return nil, last.Interface().(error)
		}
		outputs = outputs[:len(outputs)-1]
	}
	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs[0].Interface(), nil
}

func isNilValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callMethodSafely(method reflect.Value, arguments []reflect.Value) (outputs []reflect.Value, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("remote method panicked: %v", recovered)
		}
	}()
	return method.Call(arguments), nil
}

// Client calls methods on a remote Server.
type Client struct {
	Address string
	Timeout time.Duration
}

// NewClient creates a client for a server address.
func NewClient(address string) (*Client, error) {
	if address == "" {
		return nil, errors.New("address cannot be empty")
	}
	return &Client{Address: address, Timeout: defaultCallTimeout}, nil
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.
func (c *Client) Call(method string, out any, args ...any) error {
	// check validity of method name
	if method == "" {
		return errors.New("invalid method name")
	}

	// initialize connection with timeout
	conn, err := net.DialTimeout("tcp", c.Address, c.Timeout)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", c.Address, err)
	}
	defer conn.Close()

	// set deadline for the connection
	err = conn.SetDeadline(time.Now().Add(c.Timeout))
	if err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	// encode the request to be sent to the server
	encoder := json.NewEncoder(conn)
	req := request{
		Method: method,
		Args:   make([]json.RawMessage, len(args)),
	}
	for i, arg := range args {
		data, err := json.Marshal(arg)
		if err != nil {
			return fmt.Errorf("marshal argument %d: %w", i, err)
		}
		req.Args[i] = data
	}

	err = encoder.Encode(req)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	// decoding the response of the server
	decoder := json.NewDecoder(conn)
	var resp response
	err = decoder.Decode(&resp)
	if err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}

	if out == nil {
		return nil
	}

	if err := json.Unmarshal(resp.Value, out); err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	return nil

	// return errors.New("TODO: implement Client.Call")

}

package distro
import (
		"bufio"
		"encoding/json"
		"errors"
		"io"
		"log"
		"net"
		"net/http"
		"strings"
	   )

// Each client represent an RPC client. A client may contains multiple calls 
// and be called by multiple goroutines.
type Client struct {
	mu				*Mutex				// protects call map
	sending			*Mutex				// protects net.Conn to send requests in order
	seq 			uint64				// current maximum seq number
	codec			ClientCodec			// for a client, it contains only one codec
	pendingCalls	map[uint64]*Call	// pending calls
	closing			bool				// mark if a connection has been shutdown
}

// Call represents an acitve RPC call.
type Call struct {	
	Service			string					// The name of service to call
	Args			[]interface{}			// Arugments to call
	Replys			[]interface{}			// Replys to call
	Done			chan *Call			// Strobes when call is complete
	Error			error				// if error happens
	seq 			uint64				// Seq number of the call
}

type ClientCodec interface {
	WriteRequest(*Request) error
	ReadResponse(*Response) error
	Close() error
}

type goClientCodec struct {
	rwc 	io.ReadWriteCloser
	dec 	*json.Decoder
	enc 	*json.Encoder
	encBuf 	*bufio.Writer
}

func (codec *goClientCodec) WriteRequest(req *Request) (err error) {
	if err = codec.enc.Encode(req); err != nil {
		return
	}
	return codec.encBuf.Flush()
}

func (codec *goClientCodec) ReadResponse(resp *Response) (err error) {
	return codec.dec.Decode(resp)
}

func (codec *goClientCodec) Close() error {
	return codec.rwc.Close()
}

func DialHTTP(network, address string) (*Client, error) {
	return DialHTTPPath(network, address, DefaultRPCPath) 
}

func DialHTTPPath(network, address, path string) (*Client, error) {
	var err error
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	// send http header if dial successfully
	io.WriteString(conn, "CONNECT "+path+" HTTP/1.0\n\n")

	// require successful HTTP response before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method:"CONNECT"})
	status := resp.Status
	status = strings.TrimSpace(status)
	if err == nil && status == connected {
		log.Print("client: dial success")
		return NewClient(conn), nil
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	conn.Close()
	return nil, err
}

func Dial(network, address string) (*Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), nil
}

func NewClient(conn io.ReadWriteCloser) *Client {
	buf := bufio.NewWriter(conn)
	codec := &goClientCodec{conn, json.NewDecoder(conn), json.NewEncoder(buf), buf}
	return NewClientWithCodec(codec)
}

func NewClientWithCodec(cdc ClientCodec) *Client {
	cli := &Client{mu: NewMutex(), sending: NewMutex(), codec: cdc, pendingCalls: make(map[uint64]*Call)} 
	go cli.input()
	return cli
}

// Asynchronous call
func (cli *Client) Call(fnName string, args ...interface{})(call *Call) {
	call = new(Call)
	call.Service = fnName
	call.Args = args
	call.Done = make(chan *Call, 5)
	go cli.send(call)
	return
}

// unmarshal arguments to string: param1, param2... -> string1, string2...
func argsToStr(ins []interface{}) (outs []string, err error) {
	outs = make([]string, len(ins))
	for i := range ins {
		bs, err := json.Marshal(ins[i])
		if err != nil {
			return nil, err
		}
		outs[i] = string(bs)
	}
	return
}

// invoked by cli.Call
func (cli *Client) send(call *Call) {
	if call.Done == nil {
		call.Done = make(chan *Call, 5)
	}
	args, err := argsToStr(call.Args)
	if err != nil {
		call.Error = err
		call.Done <- call
		return
	}

	cli.mu.Lock()
	if cli.closing {		// connection is closed
		err = errors.New("connect is closed")
		errCalls(call, err)	
		cli.mu.Unlock()
		return
	}
	call.seq = cli.seq
	cli.seq++
	cli.pendingCalls[call.seq] = call
	cli.mu.Unlock()

	req := new(Request)	
	req.ServName = call.Service
	req.Seq = call.seq
	req.Args = args
	err = cli.codec.WriteRequest(req)
	cli.sending.Unlock()
	if err != nil {
		err := errors.New("connect is closed")
		errCalls(call, err)
	}
}

func errCalls(call *Call, err error) {
	call.Error = err
	call.Done <- call
}

// each client has only one input function running in background
func (cli *Client) input() {
	var err error
	for !cli.closing {
		resp := new(Response)
		err = cli.codec.ReadResponse(resp)
		
		seq := resp.Seq
		cli.mu.Lock()
		call := cli.pendingCalls[seq]
		if call != nil {
			delete(cli.pendingCalls, seq)
		}
		cli.mu.Unlock()
		if err != nil {			// if error happens, no only Error will be reflected to the call. No arguments will be changed
			if call != nil {
				call.Error = err
				call.Done <- call
			}
			break
		}
		if call == nil {		// this call is not pending
			continue
		}
		strToInters(call.Args, resp.Args)
		call.Replys = resp.Replys		
		if msg := resp.Error; msg != "" {
			call.Error = errors.New(msg)
		}
		call.Done <- call
	}
	cli.close(err)
}

// unmarshal args from response to call. []interface -> []string
func strToInters(ins []interface{}, args []string) {
	for i := range args {
		json.Unmarshal([]byte(args[i]), &ins[i])
	}
}

// close the session
func (cli *Client) Close() {
	cli.close(nil)
}

func (cli *Client) close(err error) {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	if cli.closing {
		return
	}
	cli.codec.Close()
	cli.closing = true		
	for _, call := range cli.pendingCalls {		// terminating pending calls
		call.Error = err
		call.Done <- call
	}	
}	

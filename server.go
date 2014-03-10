package distro
import (
		"bufio"
		"encoding/json"
		"errors"
		"fmt"
		"io"
		"log"
		"net"
		"net/http"
		"reflect"
		"runtime"
		"strings"
		"sync"
		"unicode"
		"unicode/utf8"
	   )


// resembles a method or a function with parameter types and reply types
type method struct {
	Func		reflect.Value		// resembles a function or method
	Args		[]reflect.Type		// parameter types. If func is a method, then first is a receiver 
	Replys 		[]reflect.Type		// reply types
}

// Ordinary function is a special method with no receiver.
type service struct {
	name		string				// name of service. If it is an ordinary function, then it is package name
	//ArgType		reflect.Type		// type of the receiver
	methodMap	map[string]*method	// methods for the type
}

type Server struct {
	mu				sync.RWMutex		// protect serviceMap
	serviceMap 		map[string]*service	// services that the server provide
}

// request: request header, request body
type Request struct {
	ServName	string
	Seq			uint64
	Args		[]string
}

// response: request header, request body
type Response struct {
	ServName	string
	Seq			uint64
	Error		string
	Args		[]string
	Replys		[]interface{}
}

// default encode and decode provider
type goServCodec struct {
	rwc		io.ReadWriteCloser
	dec		*json.Decoder
	enc		*json.Encoder
	encBuf	*bufio.Writer
}

type ServerCodec interface {
	ReadRequest(*Request) error 
	WriteResponse(*Response) error
	Close() error
}

const (
		DefaultRPCPath = "/_goRPC_"
		DefaultDebugPath = "/_goRPCDebug_"
	  )

var DefaultServer = NewServer()
func NewServer() *Server {
	return &Server{serviceMap: make(map[string]*service)}
}

// register serivce using default server. rcvr can be functions or pointer to types
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

// register serivce. rcvr can be functions or pointer to types
func (svr *Server) Register(rcvr interface{}) error {
	svr.mu.Lock()
	defer svr.mu.Unlock()
	if reflect.ValueOf(rcvr).IsValid() == false {
		return errors.New("invalid value " + reflect.ValueOf(rcvr).String())
	}
	if svr.serviceMap == nil {
		svr.serviceMap = make(map[string]*service)
	}
	typ := reflect.TypeOf(rcvr)
	if typ.Kind() == reflect.Func {
		return svr.registerFunc(rcvr)
	} else {
		return svr.registerType(rcvr)
	}
}

// register ordinary functions. 
func (svr *Server) registerFunc(rcvr interface{}) error {
	typ := reflect.TypeOf(rcvr)
	if typ.Kind() != reflect.Func {
		return errors.New("rpc.Register: can not register type as functions: " + typ.String())	
	}
	var name = runtime.FuncForPC(reflect.ValueOf(rcvr).Pointer()).Name()
	if name == "" {
		return errors.New("func name is empty")
	}
	dot := strings.Index(name, ".")				// seperate main.func as main func
	if dot == -1 {
		return errors.New("rpc.Register: You have to include package name to register " + name)
	}
	pkgName := name[0:dot]
	funcName := name[dot+1:]
	if s, present := svr.serviceMap[pkgName]; present {
		if _, present2 := s.methodMap[funcName]; present2 {
			return errors.New("rpc.Register: " + name + " registered already")
		}
	}

	var me method
	me.Func = reflect.ValueOf(rcvr)
	if typ.NumIn() > 0 {
		var args []reflect.Type
		for i := 0; i < typ.NumIn(); i++ {
			args = append(args, typ.In(i))
		}
		me.Args = args
	} 
	if typ.NumOut() > 0 {
		var replys []reflect.Type
		for i := 0; i < typ.NumOut(); i++ {
			replys = append(replys, typ.Out(i))
		}
		me.Replys = replys
	}
	s, ok := svr.serviceMap[pkgName]
	if !ok {			// no functions of the same package. Allocate memory for new members
		s = new(service)
		s.name = pkgName
		svr.serviceMap[pkgName] = s
		mp := make(map[string]*method)
		s.methodMap = mp
	}
	mp := s.methodMap
	mp[funcName] = &me
	
	return nil
}

// register types. rcvr must be a pointer to exported types
func (svr *Server) registerType(rcvr interface{}) error {	
	name := reflect.Indirect(reflect.ValueOf(rcvr)).Type().Name()			// get the real type name in case rcvr is a pointer
	if name == "" {
		return errors.New("rpc.Register no service name for type " + name)
	}
	if !isExported(name) {												// prevent the type is not exported
		return errors.New("rpc.Register type: " + name + " is not exported.")
	}
	if _, isPresent := svr.serviceMap[name]; isPresent {
		return errors.New("rpc.Register: service already defined:" + name)
	}
	typ := reflect.TypeOf(rcvr)
	meMp := suitableMethod(typ)	
	if len(meMp) == 0 {
		if typ.Kind() == reflect.Ptr {
			return errors.New("rcp.Register: no suitable method for type " + typ.Name())
		} else {
			meMp = suitableMethod(reflect.PtrTo(typ))
			if len(meMp) != 0 {
				return errors.New("rcp.Register: no suitable method for type " + typ.Name() + ". try pointer type")
			} else {
				return errors.New("rcp.Register: no suitable method for type " + typ.Name())
			}
		}
	}
	s := new(service)
	s.name = name
	s.methodMap = meMp
	svr.serviceMap[name] = s
	return nil
}

// get methods of type
func suitableMethod(typ reflect.Type) map[string]*method {
	methods := make(map[string]*method)
	for i := 0; i < typ.NumMethod(); i++ {
		me := typ.Method(i)
		mtyp := me.Type
		if me.PkgPath != "" {				// empty for lower case methods
			continue
		}
		var args []reflect.Type
		for i := 0; i < mtyp.NumIn(); i++ {
			args = append(args, mtyp.In(i))
		}
		var replys []reflect.Type
		for i := 0; i < mtyp.NumOut(); i++ {
			replys = append(replys, mtyp.Out(i))
		}	
		methods[me.Name] = &method{me.Func, args, replys}
	}
	return methods
}

func HandleHTTP() {
	DefaultServer.HandleHTTP(DefaultRPCPath, DefaultDebugPath)
}

// TODO: add debug feature
func (svr *Server) HandleHTTP(rpcPath, debugPath string) {
	http.Handle(rpcPath, svr)
	//http.Handle(debugPath, debugHTTP{svr})	
}

var connected = "200 connected to RPC"
// to implement http.Handler interface, which is used in http.Handle
func (svr *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()		// take over control of the connection
	if err != nil {
		return
	}
	io.WriteString(conn, "HTTP/1.0 " + connected +" \n\n")
	svr.ServeConn(conn)
}

// for tcp rpc
func ServeConn(conn io.ReadWriteCloser) {
	log.Print("connection from ", conn)
	DefaultServer.ServeConn(conn)
}

// ServeConn runs the server on a single connection.
// ServeConn bocks, serving the connection until the client hangs up.
// The caller typically invokes ServeConn in a go statement.
// ServeConn uses the gob wire format on the connection.
// To use an alternate codc, use ServeCodec. 
func (svr *Server) ServeConn(conn io.ReadWriteCloser) {
	buf := bufio.NewWriter(conn)
	codec := &goServCodec{conn, json.NewDecoder(conn), json.NewEncoder(buf), buf}
	svr.ServeCodec(codec)
}

// check if a function is exported or not
func isExported(name string) bool {
	if name == "" {
		return false
	}
	r, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(r)
}

// ServeCodec is like ServeConn but uses specified codec to to decode request and encode response.
func (svr *Server) ServeCodec(codec ServerCodec) {
	sending := NewMutex()
	for {
		req, keepReading, err := svr.readRequest(codec)
		if err == nil && keepReading {
			go svr.call(sending, req, codec)
			continue
		}
		if err != nil && err == io.EOF {
			log.Print(err)
		}
		if !keepReading {
			break
		}
	}	
	codec.Close()
}

// get the request
func (svr *Server) readRequest(codec ServerCodec) (req *Request, keepReading bool, err error) {
	req = new(Request)
	err = codec.ReadRequest(req)
	if err != nil {
		err = errors.New("rpc: server cannot decode request: " + err.Error())
		return
	}
	keepReading = true
	return
}

// typically called in a go statement
func (svr *Server) call(sending *Mutex, req *Request, codec ServerCodec) {
	var srvcName, meName string
	var msg string
	var resp Response
	resp.ServName = req.ServName
	resp.Seq = req.Seq

	dot := strings.Index(req.ServName, ".")
	if dot == -1 {
		srvcName = "main"
		meName = req.ServName
	} else {
		srvcName = req.ServName[0:dot]
		meName = req.ServName[dot+1:]
	}	

	srvc, ok := svr.serviceMap[srvcName]
	var me *method
	if !ok {
		msg = "rpc Server: no such type: " + srvcName
	} else {
		me, ok = srvc.methodMap[meName]
		if !ok {
			msg = "rcp server: no method " + meName + " for type " + srvcName
		}
	}
	if msg != "" {
		svr.sendInvalidResponse(sending, req, msg, codec)
		return
	}
	done := make(chan interface{}, 1)
	var replys []reflect.Value
	// transform req.Args, whose type is []interface{}, to []reflect.Value

	args, err := getArgsReady(req.Args, me)
	if err  != nil {
		svr.sendInvalidResponse(sending, req, err.Error(), codec)
		return
	}
	go func() {
		defer func() {
			e := recover()
			done <- e
		}()

		replys = me.Func.Call(args)
		resp.Replys = make([]interface{}, len(replys))
		for i, v := range replys {
			resp.Replys[i] = v.Interface()
		}
		resp.Args, _ = valueToStr(args)

	}()
	em := <- done
	if em != nil {
		msg = fmt.Sprintf("rpc error: %v", em)
		svr.sendInvalidResponse(sending, req, msg, codec)
	} 
	svr.sendResponse(sending, &resp, codec)
}

// marshal arguments from []reflect.Value to []string, 
// which will be sent back to clients: []reflect.Value -> []string
func valueToStr(ins []reflect.Value) (outs []string, err error) {
	outs = make([]string, len(ins))
	for i := range ins {
		val, _ := json.Marshal(ins[i].Interface())
		outs[i] = string(val)
	}
	return
}

// transfer argument contained in request from []string to []reflect.Value, 
// which can be used in function.Call:[]string -> []reflect.Value
func getArgsReady(ins []string, me *method) (outs []reflect.Value, err error) {
	if !me.Func.Type().IsVariadic() {		// non variadic functions
		return argsToValue(ins, me.Args)
	}
	inVarLen := len(ins)
	orgVarLen := len(me.Args)
	switch {
	case inVarLen < orgVarLen - 1:	// not enough parameters
		return nil, errors.New("No engough parameters for variadic functions")
	default:		// have parameters ready for variadic one
		outs, err = argsToValue(ins[0:len(me.Args)-1], me.Args[0:len(me.Args)-1])
		if err != nil {
			return nil, err
		}
		lastIns := ins[len(me.Args)-1:]			// for variadic paremters
		var rules = make([]reflect.Type, len(ins) - len(me.Args) + 1)
		for i := range rules {
			rules[i] = me.Args[len(me.Args)-1].Elem()
		}
		last, err := argsToValue(lastIns, rules)
		if err != nil {
			return nil, err
		}
		outs = append(outs, last...)
		return outs, nil
	}
}
	
// unmarshal requests paramters wrapped in string to function arguments: []string -> []reflect.Value
func argsToValue(ins []string, rules []reflect.Type) (outs []reflect.Value, err error) {
	outs = make([]reflect.Value, len(ins))
	for i := range ins {
		typ := rules[i]
		bs := []byte(ins[i])
		var argv reflect.Value
		argIsValue := false
		if typ.Kind() == reflect.Ptr {
			argv = reflect.New(typ.Elem())
		} else {
			argv = reflect.New(typ)
			argIsValue = true
		}
		// argv guaranteed to be a pointer now
		if err = json.Unmarshal(bs, argv.Interface()); err != nil {
			return 
		}
		if argIsValue {
			argv = argv.Elem()
		}
		outs[i] = argv
	}
	return
}

// if error occurs, send an invliad response to client.
func (svr *Server) sendInvalidResponse(sending *Mutex, req *Request, msg string, codec ServerCodec) {
	var resp Response
	resp.ServName = req.ServName
	resp.Seq = req.Seq
	resp.Error = msg
	svr.sendResponse(sending, &resp, codec)
}

// send response to client
func (svr *Server) sendResponse(sending *Mutex, resp *Response, codec ServerCodec) {
	sending.Lock()
	defer sending.Unlock()
	if err := codec.WriteResponse(resp); err != nil {
		log.Print("rpc: writing response" + err.Error())
	}
}

func (codec *goServCodec) ReadRequest(req *Request) error {
	return codec.dec.Decode(req)
}

func (codec *goServCodec) WriteResponse(resp *Response) (err error) {
	if err = codec.enc.Encode(resp); err != nil {
		return 
	}
	return codec.encBuf.Flush()
}

func (codec *goServCodec) Close() error {
	return codec.rwc.Close()
}

func (svr *goServCodec) Accept(lis net.Listener) (net.Conn, error) {
	return lis.Accept()	
}

func Accept(lis net.Listener) (net.Conn, error) {
	return lis.Accept()
}

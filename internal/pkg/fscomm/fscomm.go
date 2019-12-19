/*
 * Copyright (c) 2018-2019 Geoffroy Vallee, All rights reserved
 * Copyright (c) 2019 Sylabs, Inc. All rights reserved
 * This software is licensed under a 3-clause BSD license. Please consult the
 * LICENSE.md file distributed with the sources of this project regarding your
 * rights to use or distribute this software.
 */

package fscomm

import (
	"encoding/binary"
	"log"
	"net"
	"time"

	"github.com/gvallee/syserror/pkg/syserror"
)

// Messages types
var INVALID = "INVA" // Invalid msg
var TERMMSG = "TERM" // Termination message
var CONNREQ = "CONN" // Connection request (initiate a connection handshake
var CONNACK = "CACK" // Response to a connection request (Connection ack)
var DATAMSG = "DATA" // Data msg, associated to a write operation
var READREQ = "READ" // Data request, associated to a read operation
var RDREPLY = "RRPL" // Reply to a READREQ, including the data

// ServerInfo is the structure to store server information (host we connect to)
type ServerInfo struct {
	conn      net.Conn
	url       string
	blocksize uint64
	timeout   int // Timeout for accept(), no timeout if set to zero
}

// LocalServer is the state of the local server if it exists (ATM we can be only one server at a time)
var LocalServer *ServerInfo = nil

/**
 * GetConnFromInfo returns the connection from the server info structure. This allows us to have
 * external components access the connection structure and use it
 * @param[in]   info    Data server info's data structure
 * @return      Connection structure
 * @return      System error handle
 */
func GetConnFromInfo(info *ServerInfo) (net.Conn, syserror.SysError) {
	if info == nil {
		return nil, syserror.ErrNotAvailable
	}
	return info.conn, syserror.NoErr
}

/**
 * GetHeader receives and parses a message header (4 character)
 * @param[in]	conn	Active connection from which to receive data
 * @return	Message type (string)
 * @return	System error handler
 */
func GetHeader(conn net.Conn) (string, syserror.SysError) {
	if conn == nil {
		return INVALID, syserror.ErrNotAvailable
	}

	hdr := make([]byte, 4)
	if hdr == nil {
		return INVALID, syserror.ErrOutOfRes
	}

	/* read the msg type */
	s, myerr := conn.Read(hdr)
	if myerr != nil {
		log.Println("ERROR:", myerr.Error())
		return INVALID, syserror.ErrFatal
	}

	// Connection is closed
	if s == 0 {
		log.Println("Connection closed")
		return TERMMSG, syserror.NoErr
	}

	// Read returns an error (we test this only as a second test since s=0 means socket closed, whic returns also EOF as an error but we want to handle it as normal termination, not an error case
	if myerr != nil {
		log.Println("ERROR:", myerr.Error())
		return INVALID, syserror.ErrFatal
	}

	if s > 0 && s != 4 {
		log.Println("ERROR Cannot recv header")
		return INVALID, syserror.NoErr
	}

	// Disconnect request
	if string(hdr[:s]) == TERMMSG {
		log.Println("Recv'd disconnect request")
		return TERMMSG, syserror.NoErr
	}

	if string(hdr[:s]) == CONNREQ {
		log.Println("Recv'd connection request")
		return CONNREQ, syserror.NoErr
	}

	if string(hdr[:s]) == CONNACK {
		log.Println("Recv'd connection ACK")
		return CONNACK, syserror.NoErr
	}

	if string(hdr[:s]) == DATAMSG {
		log.Println("Recv'd data message")
		return DATAMSG, syserror.NoErr
	}

	if string(hdr[:s]) == READREQ {
		log.Println("Recv'd read request")
		return READREQ, syserror.NoErr
	}

	if string(hdr[:s]) == RDREPLY {
		log.Println("Recv'd read reply")
		return RDREPLY, syserror.NoErr
	}

	log.Println("Invalid msg header")
	return INVALID, syserror.ErrFatal
}

/**
 * Receive the payload size.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Payload size (can be zero)
 * @return	System error handle
 */
func getPayloadSize(conn net.Conn) (uint64, syserror.SysError) {
	ps := make([]byte, 8) // Payload size is always 8 bytes
	s, myerr := conn.Read(ps)
	if s != 8 || myerr != nil {
		log.Println("ERROR: expecting 8 bytes but received", s)
		return 0, syserror.ErrFatal
	}

	return binary.LittleEndian.Uint64(ps), syserror.NoErr
}

/**
 * Receive the payload.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Payload in []byte
 * @return	System error handle
 */
func getPayload(conn net.Conn, size uint64) ([]byte, syserror.SysError) {
	payload := make([]byte, size)
	s, myerr := conn.Read(payload)
	if uint64(s) != size || myerr != nil {
		log.Println("ERROR: expecting ", size, "but received", s)
		return nil, syserror.ErrFatal
	}

	return payload, syserror.NoErr
}

/**
 * HandleReadReq is called upon the reception of a read request. It is assumed that the message
 * header already has been received. This returns all the necessary data required by the
 * data server to read and return data from a given block.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Namespace's name of the read request
 * @return	Block id of the read request
 * @return	Offset of the read request
 * @return	Size of the read request
 * @return	System error handle
 */
func HandleReadReq(conn net.Conn) (string, uint64, uint64, uint64, syserror.SysError) {
	var namespace_len uint64
	var namespace string
	var blockid uint64
	var offset uint64
	var size uint64
	var myerr syserror.SysError

	// Recv the length of the namespace
	namespace_len, myerr = RecvUint64(conn)
	if myerr != syserror.NoErr {
		return "", 0, 0, 0, myerr
	}

	// Recv the namespace
	namespace, myerr = RecvNamespace(conn, namespace_len)
	if myerr != syserror.NoErr {
		return "", 0, 0, 0, myerr
	}

	// Recv the blockid (8 bytes)
	blockid, myerr = RecvUint64(conn)
	if myerr != syserror.NoErr {
		return "", 0, 0, 0, myerr
	}

	// Recv the offset
	offset, myerr = RecvUint64(conn)
	if myerr != syserror.NoErr {
		return "", 0, 0, 0, myerr
	}

	// Recv data size
	size, myerr = RecvUint64(conn)
	if myerr != syserror.NoErr {
		return "", 0, 0, 0, myerr
	}

	return namespace, blockid, offset, size, syserror.NoErr
}

/**
 * Handle a connection request message, i.e., send a CONNACK message with the blocksize
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	size	Size of the payload included in the CONNREQ message
 * @param[in]	payload	Payload included in the CONNREQ message
 * @return	System error handle
 */
func handleConnReq(conn net.Conn, size uint64, payload []byte) syserror.SysError {
	if LocalServer == nil {
		log.Println("ERROR: local server is not initialized")
		return syserror.ErrFatal
	}

	// Send CONNACK with the block size
	buff := make([]byte, 8) // 8 bytes for the block size
	binary.LittleEndian.PutUint64(buff, LocalServer.blocksize)
	syserr := SendMsg(conn, CONNACK, buff)
	if syserr != syserror.NoErr {
		return syserr
	}

	return syserror.NoErr
}

/**
 * HandleHandshake receives and handles a CONNREQ message, i.e., a client trying to connect
 * @param[in]   conn    Active connection from which to receive data
 * @return	System error handle
 */
func HandleHandshake(conn net.Conn) syserror.SysError {
	/* Handle the CONNREQ message */
	msgtype, payload_size, payload, syserr := RecvMsg(conn)
	if syserr != syserror.NoErr {
		return syserror.ErrFatal
	}

	if msgtype == CONNREQ {
		handleConnReq(conn, payload_size, payload)
	} else {
		return syserror.NoErr // We did not get the expected CONNREQ msg
	}

	return syserror.NoErr
}

/**
 * Create an embeded server loop, i.e., an internal server that only receives and
 * do a basic parsing of messages. Mainly for testing.
 * @param[in]   conn    Active connection from which to receive data
 * @return	System error handle
 */
func doServer(conn net.Conn) syserror.SysError {
	done := 0

	syserr := HandleHandshake(conn)
	if syserr != syserror.NoErr {
		log.Println("ERROR: Error during handshake with client")
	}
	for done != 1 {
		// Handle the message header
		msgtype, syserr := GetHeader(conn)
		if syserr != syserror.NoErr {
			done = 1
		}
		if msgtype == "INVA" || msgtype == "TERM" {
			done = 1
		}

		//  Handle the paylaod size
		payload_size, pserr := getPayloadSize(conn)
		if pserr != syserror.NoErr {
			done = 1
		}

		// Handle the payload when necessary
		var payload []byte = nil
		if payload_size != 0 {
			payload, syserr = getPayload(conn, payload_size)
		}
		if payload_size != 0 && payload != nil {
			return syserror.ErrFatal
		}
	}

	conn.Close()
	FiniServer()

	return syserror.NoErr
}

/**
 * FiniServer cleanly finalizes a server.
 */
func FiniServer() {
	LocalServer = nil
}

/**
 * CreateEmbeddedServer creates an embedded server, mainly for testing since we cannot customize how messages are
 * handled.
 * @param[in]	info	Structure representing the information about the server to create.
 * @return	System error handle
 */
func CreateEmbeddedServer(info *ServerInfo) syserror.SysError {
	if info == nil {
		return syserror.ErrFatal
	}

	if LocalServer != nil {
		log.Println("ERROR: Local server already instantiated")
		return syserror.ErrFatal
	}

	LocalServer = info
	listener, myerr := net.Listen("tcp", info.url)
	if myerr != nil {
		return syserror.ErrFatal
	}

	log.Println("Server created on", info.url)

	var conn net.Conn
	for {
		conn, myerr = listener.Accept()
		info.conn = conn
		if myerr != nil {
			log.Println("ERROR: ", myerr.Error())
		}

		go doServer(conn)
	}

	// todo: implement clean termination for the embedded server
	//return syserror.NoErr
}

/**
 * CreateServer creates a generic server. Note that the connection handle from clients need to be explicitely
 * added to the server's code.
 * @param[in]	info	Structure representing the information about the server to create.
 * @return	Systemn error handle
 */
func CreateServer(info *ServerInfo) syserror.SysError {
	if info == nil {
		return syserror.ErrFatal
	}

	if LocalServer != nil {
		log.Println("ERROR: Local server already instantiated")
		return syserror.ErrFatal
	}

	LocalServer = info
	listener, myerr := net.Listen("tcp", info.url)
	if myerr != nil {
		log.Println(myerr.Error())
		return syserror.ErrFatal
	}

	if info.timeout > 0 {
		listener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Duration(info.timeout) * time.Second))
	}

	info.conn, myerr = listener.Accept()

	return syserror.NoErr
}

/**
 * Connect2Server connects to a server that is listening for incoming connection request
 * @param[in[	url	URL of the server to connect to
 * @return	Connection handle to the server
 * @return	Data server's block size
 * @return	System error handle
 */
func Connect2Server(url string) (net.Conn, uint64, syserror.SysError) {
	retry := 0

Retry:
	conn, neterr := net.Dial("tcp", url)
	if neterr != nil {
		log.Println("[WANR] ", neterr.Error())
		if retry < 5 {
			log.Printf("Retrying after %d seconds\n", retry)
			retry++
			time.Sleep(time.Duration(retry) * time.Second)
			goto Retry
		}
		return nil, 0, syserror.ErrOutOfRes
	}

	bs, myerr := ConnectHandshake(conn)
	if bs == 0 || myerr != syserror.NoErr {
		return nil, 0, syserror.ErrFatal
	}

	return conn, bs, syserror.NoErr
}

/**
 * Send a 8 bytes integer. Extremely useful to exchange IDs.
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	value	Value to send (uint64)
 * @return	System error handle
 */
func sendUint64(conn net.Conn, value uint64) syserror.SysError {
	if conn == nil {
		return syserror.ErrFatal
	}

	buff := make([]byte, 8)
	if buff == nil {
		return syserror.ErrOutOfRes
	}

	binary.LittleEndian.PutUint64(buff, value)
	s, myerr := conn.Write(buff)
	if myerr != nil {
		log.Println(myerr.Error())
	}
	if myerr == nil && s != 8 {
		log.Println("Received ", s, "bytes, instead of 8")
	}
	if s != 8 || myerr != nil {
		return syserror.ErrFatal
	}

	return syserror.NoErr
}

/**
 * RecvUint64 receives a 8 bytes integer. Extremely usefuol to exchange IDs.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Received 8 bytes integer
 * @return	System error handle
 */
func RecvUint64(conn net.Conn) (uint64, syserror.SysError) {
	if conn == nil {
		return 0, syserror.ErrFatal
	}

	msg := make([]byte, 8)
	if msg == nil {
		return 0, syserror.ErrOutOfRes
	}

	s, myerr := conn.Read(msg)
	if s != 8 || myerr != nil || msg == nil {
		return 0, syserror.ErrFatal
	}

	return binary.LittleEndian.Uint64(msg), syserror.NoErr
}

/**
 * Send the message type (4 bytes)
 * @param[in]   conn    Active connection from which to receive data
 * @parma[in]	msgType	Msg type to send
 * @return	System error handle
 */
func sendMsgType(conn net.Conn, msgType string) syserror.SysError {
	if conn == nil {
		log.Println("[ERROR] connection not defined")
		return syserror.ErrFatal
	}

	s, myerr := conn.Write([]byte(msgType))
	if s == 0 || myerr != nil {
		log.Printf("[ERROR] unable to send message (%d bytes sent)", int(s))
		return syserror.ErrFatal
	}

	return syserror.NoErr
}

/**
 * Receive a message type (4 bytes)
 * @param[in]   conn    Active connection from which to receive data
 * @return	Message type (string)
 * @return	System error handle
 */
func recvMsgType(conn net.Conn) (string, syserror.SysError) {
	if conn == nil {
		return "", syserror.ErrFatal
	}

	msgtype, syserr := GetHeader(conn)
	if syserr != syserror.NoErr {
		return "", syserror.ErrFatal
	}

	return msgtype, syserror.NoErr
}

/**
 * Send data.
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	data	buffer to send ([]byte)
 * @return	System error handle
 */
func sendData(conn net.Conn, data []byte) syserror.SysError {
	if conn == nil {
		return syserror.ErrFatal
	}

	s, myerr := conn.Write(data)
	if s == 0 || myerr != nil {
		return syserror.ErrFatal
	}

	return syserror.NoErr
}

/**
 * DoRecvData receives data; the receive buffer is automatically allocated.
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	size	Amount of data to receive (in bytes)
 * @return	Buffer with the received data ([]byte)
 * @return	System error handle
 */
func DoRecvData(conn net.Conn, size uint64) ([]byte, syserror.SysError) {
	data := make([]byte, size)
	if data == nil {
		return nil, syserror.ErrOutOfRes
	}

	s, myerr := conn.Read(data)
	if myerr != nil || uint64(s) != size || data == nil {
		return nil, syserror.ErrFatal
	}

	return data, syserror.NoErr
}

/**
 * Send a namespace (string)
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	namespace	Namespace's name to send
 * @return	System error handle
 */
func sendNamespace(conn net.Conn, namespace string) syserror.SysError {
	s, myerr := conn.Write([]byte(namespace))
	if s == 0 || myerr != nil {
		return syserror.ErrFatal
	}

	return syserror.NoErr
}

/**
 * RecvNamespace receives a namespace (string)
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	size	Length of the namespace's name to receive
 * @return	Namespace's name (string)
 * @return	System error handle
 */
func RecvNamespace(conn net.Conn, size uint64) (string, syserror.SysError) {
	if conn == nil {
		return "", syserror.ErrFatal
	}

	buff := make([]byte, size)
	if buff == nil {
		return "", syserror.ErrFatal
	}

	s, myerr := conn.Read(buff)
	if myerr != nil || uint64(s) != size || buff == nil {
		return "", syserror.ErrFatal
	}

	return string(buff), syserror.NoErr
}

/**
 * SendReadReq sends a read request to a data server.
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]   namespace       Namespace's name in the context of which the data is sent
 * @param[in]   blockid         Block id where the data needs to be saved
 * @param[in]   offset          Block offset where the data needs to be saved
 * @param[in]	size		Amount of data to read
 */
func SendReadReq(conn net.Conn, namespace string, blockid uint64, offset uint64, size uint64) syserror.SysError {
	// Send the msg type
	myerr := sendMsgType(conn, DATAMSG)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the length of the namespace
	var nslen uint64 = uint64(len(namespace))
	myerr = sendUint64(conn, nslen)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the namespace
	myerr = sendNamespace(conn, namespace)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the blockid (8 bytes)
	myerr = sendUint64(conn, blockid)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the offset
	myerr = sendUint64(conn, offset)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send data size
	myerr = sendUint64(conn, size)
	if myerr != syserror.NoErr {
		return myerr
	}

	return syserror.NoErr
}

/**
 * SendData sends a buffer
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	namespace	Namespace's name in the context of which the data is sent
 * @param[in] 	blockid		Block id where the data needs to be saved
 * @param[in]	offset		Block offset where the data needs to be saved
 * @param[in]	data		Data to send
 * @return	System error handle
 */
func SendData(conn net.Conn, namespace string, blockid uint64, offset uint64, data []byte) syserror.SysError {
	// Send the msg type
	myerr := sendMsgType(conn, DATAMSG)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the length of the namespace
	var nslen uint64 = uint64(len(namespace))
	myerr = sendUint64(conn, nslen)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the namespace
	myerr = sendNamespace(conn, namespace)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the blockid (8 bytes)
	myerr = sendUint64(conn, blockid)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the offset
	myerr = sendUint64(conn, offset)
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send data size
	myerr = sendUint64(conn, uint64(len(data)))
	if myerr != syserror.NoErr {
		return myerr
	}

	// Send the actual data
	myerr = sendData(conn, data)
	if myerr != syserror.NoErr {
		return myerr
	}

	return syserror.NoErr
}

/**
 * SendMsg sends a basic message
 * @param[in]   conn    Active connection from which to receive data
 * @param[in]	msgType	Type of the message to send
 * @param[in]	Payload to associte to the message
 * @return 	System error handle
 */
func SendMsg(conn net.Conn, msgType string, payload []byte) syserror.SysError {
	if conn == nil {
		log.Println("[ERROR] connection is not defined")
		return syserror.ErrFatal
	}

	hdrerr := sendMsgType(conn, msgType)
	if hdrerr != syserror.NoErr {
		log.Println("failed to send message type")
		return hdrerr
	}

	if payload != nil {
		syserr := sendUint64(conn, uint64(len(payload)))
		if syserr != syserror.NoErr {
			log.Println("failed to send payload size")
			return syserror.ErrFatal
		}
		conn.Write(payload)
	} else {
		syserr := sendUint64(conn, uint64(0))
		if syserr != syserror.NoErr {
			log.Println("failed to send payload size")
			return syserror.ErrFatal
		}
	}

	return syserror.NoErr
}

/**
 * RecvData receives data to be stored
 * @param[in]   conn    Active connection from which to receive data
 * @return	Namespace's name for which the data is received
 * @return	Blockid where the data needs to be saved
 * @return	Block offset where the data needs to be saved
 * @return	Buffer with the received data
 * @return	System error handle
 */
func RecvData(conn net.Conn) (string, uint64, uint64, []byte, syserror.SysError) {
	// Recv the msg type
	msgtype, terr := recvMsgType(conn)
	if terr != syserror.NoErr || msgtype != DATAMSG {
		return "", 0, 0, nil, terr
	}

	// Recv the length of the namespace
	nslen, syserr := RecvUint64(conn)
	if syserr != syserror.NoErr {
		return "", 0, 0, nil, syserr
	}

	// Recv the namespace
	namespace, nserr := RecvNamespace(conn, nslen)
	if nserr != syserror.NoErr {
		return "", 0, 0, nil, nserr
	}

	// Recv blockid
	blockid, berr := RecvUint64(conn)
	if berr != syserror.NoErr {
		return namespace, 0, 0, nil, berr
	}

	// Recv offset
	offset, oerr := RecvUint64(conn)
	if oerr != syserror.NoErr {
		return namespace, blockid, 0, nil, oerr
	}

	// Recv data size
	size, serr := RecvUint64(conn)
	if serr != syserror.NoErr {
		return namespace, blockid, offset, nil, serr
	}

	// Recv the actual data
	data, derr := DoRecvData(conn, size)
	if derr != syserror.NoErr {
		return namespace, blockid, offset, nil, derr
	}

	return namespace, blockid, offset, data, syserror.NoErr
}

/**
 * RecvMsg receives a generic message
 * @param[in]   conn    Active connection from which to receive data
 * @return	Message type
 * @return	Payload size included in the message
 * @return	Payload ([]byte)
 * @return	System error handle
 */
func RecvMsg(conn net.Conn) (string, uint64, []byte, syserror.SysError) {
	if conn == nil {
		return "", 0, nil, syserror.ErrFatal
	}

	msgtype, myerr := GetHeader(conn)
	// Messages without payload
	if msgtype == "TERM" || msgtype == "INVA" || myerr != syserror.NoErr {
		return msgtype, 0, nil, syserror.ErrFatal
	}

	// Get the payload size
	payload_size, pserr := getPayloadSize(conn)
	if pserr != syserror.NoErr {
		return msgtype, 0, nil, syserror.ErrFatal
	}
	if payload_size == 0 {
		return msgtype, 0, nil, syserror.NoErr
	}

	// Get the payload
	buff, perr := getPayload(conn, payload_size)
	if perr != syserror.NoErr {
		return msgtype, payload_size, buff, syserror.NoErr
	}

	return msgtype, payload_size, buff, syserror.NoErr
}

/**
 * ConnectHandshake initiates a connection handshake. Required for a client to successfully connect to a server.
 * @param[in]   conn    Active connection from which to receive data
 * @return	Server's block size
 * @return	System error handle
 */
func ConnectHandshake(conn net.Conn) (uint64, syserror.SysError) {
	myerr := SendMsg(conn, CONNREQ, nil)
	if myerr != syserror.NoErr {
		return 0, myerr
	}

	// Receive the CONNACK, the payload is the block_sizw
	msgtype, s, buff, recverr := RecvMsg(conn)
	if recverr != syserror.NoErr || msgtype != CONNACK || s != 8 || buff == nil {
		return 0, syserror.ErrFatal
	}
	block_size := binary.LittleEndian.Uint64(buff)
	if LocalServer != nil {
		LocalServer.blocksize = block_size
	}

	return block_size, syserror.NoErr
}

/**
 * CreateServerInfo creates an info handle that stores all the server info.
 * @param[in]	url	URL of the targer server
 * @param[in]	blocksize	Block size of the server.
 * @return	Pointer to a server info structure.
 */
func CreateServerInfo(url string, blocksize uint64, timeout int) *ServerInfo {
	s := new(ServerInfo)
	if s == nil {
		return nil
	}

	s.blocksize = blocksize
	s.url = url
	s.timeout = timeout
	s.conn = nil

	return s
}

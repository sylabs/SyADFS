/*
 * Copyright (c) 2018-2019 Geoffroy Vallee, All rights reserved
 * Copyright (c) 2019 Sylabs, Inc. All rights reserved
 * This software is licensed under a 3-clause BSD license. Please consult the
 * LICENSE.md file distributed with the sources of this project regarding your
 * rights to use or distribute this software.
 */

package fscomm

import (
	"net"
	"testing"
	"time"

	"github.com/gvallee/syserror/pkg/syserror"
)

const namespace1 = "namespace_test1"
const namespace2 = "namespace2"
const block1 = 1
const offset1 = 128
const block2 = 42
const offset2 = 24

func sendFiniMsg(t *testing.T, conn net.Conn) {
	myerr := SendMsg(conn, TERMMSG, nil)
	if myerr != syserror.NoErr {
		t.Fatal("ERROR: SendMsg() failed")
	}

	time.Sleep(5 * time.Second) // Give a chance for the msg to arrive before we exit which could close the connection before the test completes
}

func TestServerCreation(t *testing.T) {
	t.Log("Testing creation of a valid server ")
	testURL := "127.0.0.1:8811"

	// Create a server asynchronously
	server_info := CreateServerInfo(testURL, uint64(1024), 0)
	go CreateEmbeddedServer(server_info)

	// Create a simple client that will just terminate everything
	conn, bs, connerr := Connect2Server(testURL)
	if conn == nil || bs != 1024 || connerr != syserror.NoErr {
		t.Fatal("ERROR: Cannot connect to server")
	}
	sendFiniMsg(t, conn)

}

func runServer(t *testing.T, info *ServerInfo) {
	t.Log("Actually creating the server...")
	mysyserr := CreateServer(info)
	if mysyserr != syserror.NoErr {
		t.Fatal("ERROR: Cannot create new server")
	}

	done := 0
	t.Log("Waiting for connection handshake...")
	HandleHandshake(info.conn)
	for done != 1 {
		t.Log("Receiving data...")
		namespace, blockid, offset, data, myerr := RecvData(info.conn)
		if myerr != syserror.NoErr {
			t.Fatal("ERROR: Cannot recv data", myerr.Error())
		}
		t.Log("Receiving data for namespace", namespace, "for blockid", blockid, "at offset", offset, " - ", len(data), "bytes")
		if namespace != namespace1 {
			t.Fatal("ERROR: namespace mismatch")
		}
		if blockid != block1 {
			t.Fatal("ERROR: blockid mismatch")
		}
		if offset != offset1 {
			t.Fatal("ERROR: offset mismatch")
		}

		myerr = SendData(info.conn, namespace2, block2, offset2, []byte("OKAY"))
		if myerr != syserror.NoErr {
			t.Fatal("ERROR: Cannot send data", myerr.Error())
		}

		/* Wait for the termination message */
		t.Log("Waiting for the termination message...")
		msgtype, _, _, _ := RecvMsg(info.conn)
		if msgtype != TERMMSG {
			t.Fatal("ERROR: Received wrong type of msg")
		}

		done = 1
	}
	FiniServer()
}

func TestSendRecv(t *testing.T) {
	t.Log("Testing data send/recv ")
	url := "127.0.0.1:9889"

	// Create a server asynchronously
	t.Log("Creating server...")
	server_info := CreateServerInfo(url, uint64(1024), 0)
	go runServer(t, server_info)

	// Once we know the server is up, we connect to it
	t.Log("Server up, conencting...")
	conn, bs, myerr := Connect2Server(url)
	if conn == nil || bs != 1024 || myerr != syserror.NoErr {
		t.Fatal("Client error: Cannot connect to server")
	}
	t.Log("Successfully connected to server")

	// Now we perform the communication tests
	t.Log("Sending test data...")
	buff := make([]byte, 512)
	t.Log("Sending data...")
	syserr := SendData(conn, namespace1, block1, offset1, buff)
	if syserr != syserror.NoErr {
		t.Fatal("ERROR: Impossible to send data")
	}

	t.Log("Receiving ack...")
	namespace, blockid, offset, data, rerr := RecvData(conn)
	if rerr != syserror.NoErr {
		t.Fatal("Client error: Cannot receive data", rerr.Error())
	}
	if namespace != namespace2 {
		t.Fatal("Client error: Namespace mismatch")
	}
	if blockid != block2 {
		t.Fatal("Client error: blockid mismatch")
	}
	if offset != offset2 {
		t.Fatal("Client error: offset mismatch")
	}
	if string(data) != "OKAY" {
		t.Fatal("Client error: data mismatch")
	}

	t.Log("Test completed, sending termination msg...")
	sendFiniMsg(t, conn)

	t.Log("PASS")
}

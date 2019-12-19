/*
 * Copyright (c) 2018-2019 Geoffroy Vallee, All rights reserved
 * Copyright (c) 2019 Sylabs, Inc. All rights reserved
 * This software is licensed under a 3-clause BSD license. Please consult the
 * LICENSE.md file distributed with the sources of this project regarding your
 * rights to use or distribute this software.
 */

package dataserver

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/gvallee/syserror/pkg/syserror"
	"github.com/sylabs/SyADFS/internal/pkg/fscomm"
)

func TestServerCreate(t *testing.T) {
	t.Log("Testing with an empty basedir... ")
	s1 := Init("", 0, "")
	if s1 != nil {
		t.Fatal("Test with basedir=nil failed")
	}
	t.Log("PASS")

	t.Log("Testing with an invalid path for basedir... ")
	s2 := Init("/a/crazy/path/that/does/not/exist", 1, "127.0.0.1:55555")
	if s2 != nil {
		t.Fatal("Test with basedir pointing to a not-existing directory failed")
	}
	t.Log("PASS")

	validTestPath, err := ioutil.TempDir("", "dataserver-")
	if err != nil {
		t.Fatalf("unable to create temporary directory: %s", err)
	}
	os.RemoveAll(validTestPath)
	// First make sure everything is clean for the test
	d, myerror := os.Open(validTestPath)
	if myerror == nil {
		// The directory already exist, we delete it
		defer d.Close()
		myerror = os.RemoveAll(validTestPath)
		if myerror != nil {
			t.Fatal("Cannot remove the basedir before running the tests")
		}
	}
	myerror = os.MkdirAll(validTestPath, 0700)
	if myerror != nil {
		t.Fatal("FATAL ERROR: cannot create directory for testing")
	}

	// Run the actual test with an invalid block size
	t.Log("Testing with a valid basedir and valid block size... ")
	s3 := Init(validTestPath, 0, "127.0.0.1:55544")
	if s3 != nil {
		t.Fatal("FATAL ERROR: Test with invalid block size failed")
	}

	t.Log("PASS")

	// Run the actual test with a valid configuration
	t.Log("Testing with a valid configuration...") // (please wait until the server times out)... ")
	valid_url := "127.0.0.1:4455"
	s4 := Init(validTestPath, 1024*1024, valid_url)
	if s4 == nil {
		t.Fatal("Test with valid basedir failed")
	}

	basedir, mysyserror := GetBasedir(s4)
	if mysyserror != syserror.NoErr || basedir != validTestPath {
		t.Fatal("FATAL ERROR: Cannot get the server's basedir")
	}

	blocksize, mysyserror2 := GetBlocksize(s4)
	if mysyserror2 != syserror.NoErr || blocksize != 1024*1024 {
		t.Fatal("FATAL ERROR: Cannot get block size")
	}

	// To properly terminate the server, we connect to the server and send a term message.
	// This is the correct way to interact with the server
	t.Log("\tConnecting to server for termination...")
	conn, bs, myerr := fscomm.Connect2Server(valid_url)
	if conn == nil || bs != 1024*1024 || myerr != syserror.NoErr {
		t.Fatal("ERROR: Cannot connect to the server")
	}

	senderr := fscomm.SendMsg(conn, fscomm.TERMMSG, nil)
	if senderr != syserror.NoErr {
		t.Fatal("Cannot send termination message")
	}

	// Message successfully sent, we poll for the server termination and let things happen
	for {
		if IsServerDone() == 1 {
			break
		}
	}

	// We clean up again
	t.Log("\tAll done, cleaning...")
	myerror = os.RemoveAll(validTestPath)
	if myerror != nil {
		t.Fatal("FATAL ERROR: cannot cleanup")
	}
	t.Log("PASS")
}

func checkNamespaceDir(t *testing.T, myserver *Server, name string) {
	basedir, syserr := GetBasedir(myserver)
	if syserr != syserror.NoErr {
		t.Fatal("FATAL ERROR: Cannot get server's basedir")
	}
	basedir += name
	_, myerror := os.Stat(basedir)
	if myerror != nil {
		t.Fatal("Expected namespace directory is missing")
	}
}

func writeTest(t *testing.T, myserver *Server, ns string, id uint64, size uint64, offset uint64) (uint64, syserror.SysError) {
	// Create an initialized buffer
	var c int8 = 0
	size = size * 1024 * 1024 // the size is in MB but the underlying layer are based on B
	buff := make([]byte, size)
	var s uint64 = 0

	i := 0
	for s < size {
		// Do we space to actually write the new value?
		if s+uint64(reflect.TypeOf(c).Size()) > size {
			break
		}

		buff[i] = byte(c)
		if c == 9 {
			c = 0
		} else {
			c += 1
		}

		s += uint64(reflect.TypeOf(c).Size())
		i += 1
	}

	ws, mysyserr := BlockWrite(myserver, ns, id, offset, buff)
	if mysyserr != syserror.NoErr {
		return 0, mysyserr
	}
	if uint64(ws) != s {
		t.Fatalf("unable to write all the data: %d vs %d", int(s), int(ws))
	}

	return uint64(ws), mysyserr
}

func readTest(t *testing.T, myserver *Server, ns string, id uint64, size uint64, offset uint64) (uint64, []byte, syserror.SysError) {
	// Convert size from MB to B
	size = size * 1024 * 1024

	rs, buff, mysyserr := BlockRead(myserver, ns, id, offset, size)
	if uint64(rs) != size {
		// Do not fail the test here, this is in a codepath where it should propagate the error up
		t.Logf("unable to read the entire data: %d vs. %d", int(size), int(rs))
	}

	// If we read from the begining of the file, we know what to expect and check
	// the content of the buffer
	if offset == 0 {
		var c int8 = 0
		var s uint64 = 0
		var content int8
		i := 0

		for s < size {
			content = int8(buff[i])
			if content != c {
				t.Log("ERROR: Got the wrong value ", content, " vs ", c, " pos: ", i)
				break
			}

			if c == 9 {
				c = 0
			} else {
				c += 1
			}

			i += 1
			s += uint64(reflect.TypeOf(c).Size())
		}
	}

	return uint64(rs), buff, mysyserr
}

func TestNamespaceCreation(t *testing.T) {
	validTestPath := "/tmp/ns_test/"
	// First make sure everything is clean for the test
	d, myerror := os.Open(validTestPath)
	if myerror == nil {
		// The directory already exist, we delete it
		defer d.Close()
		myerror = os.RemoveAll(validTestPath)
		if myerror != nil {
			t.Fatal("FATAL ERROR: Cannot remove basedir required for testing")
		}
	}
	myerror = os.MkdirAll(validTestPath, 0700)
	if myerror != nil {
		t.Fatal("FATAL ERROR: Cannot create the server's basedir")
	}

	// Create the data server
	validURL := "127.0.0.1:8888"
	myserver := Init(validTestPath, 1024*1024, validURL) // 1MB block size
	if myserver == nil {
		t.Fatal("FATAL ERROR: Cannot create data server")
	}

	// We check whether the directory is really there
	checkNamespaceDir(t, myserver, "default")
	t.Log("PASS")

	t.Log("Testing a custom namespace... ")
	ns2 := NamespaceInit("my_namespace_2", myserver)
	if ns2 == nil {
		t.Fatal("FATAL ERROR: Cannot create custom namespace")
	}

	// We check whether the directory is really there
	checkNamespaceDir(t, myserver, "my_namespace_2")
	t.Log("PASS")

	// Now testing data writing that should succeed
	t.Log("Testing a valid write ")
	ws, mysyserr := writeTest(t, myserver,
		"my_namespace_2", // namespace
		0,                // blockid
		1,                // Write 1MB
		0)                // Offset
	if mysyserr != syserror.NoErr {
		t.Fatal("FATAL ERROR: Valid write failed - Wrote ", ws, " bytes - ", mysyserr.Error())
	}
	t.Log("PASS")

	// Checking the content with the equivalent read
	t.Log("Testing a valid read ")
	rs, _, myreaderr := readTest(t, myserver,
		"my_namespace_2", // namespace
		0,                // blockid
		1,                // Read 1MB
		0)                // Offset
	if rs != 1*1024*1024 || mysyserr != syserror.NoErr {
		t.Fatal("FATAL ERROR: Valid read failed - Read ", rs, " bytes - ", myreaderr.Error())
	}
	t.Log("PASS")

	// Now testing data writing that should fail
	t.Log("Testing an invalid write ")
	ws, mysyserr = writeTest(t, myserver,
		"my_namespace_2", // namespace
		0,                // blockid
		1,                // Write 1MB
		1024)             // offset
	if mysyserr != syserror.ErrDataOverflow {
		t.Fatal("FATAL ERROR: Test did not fail correctly")
	}
	t.Log("PASS")

	// Now test data read that should fail
	t.Log("Testing an invalid read ")
	rs, _, myreaderr = readTest(t, myserver,
		"my_namespace_2", // namespace
		0,                // blockid
		1,                // Read 1 MB
		512)              // offset
	if myreaderr != syserror.ErrDataOverflow {
		t.Fatal("FATAL ERROR: Test was supposed to fail but succeeded")
	}
	t.Log("PASS")

	// To properly terminate the server, we connect to the server and send a term message.
	// This is the correct way to interact with the server
	t.Logf("\tConnecting to server for termination (%s)...", validURL)
	conn, blocksize, myerr := fscomm.Connect2Server(validURL)
	if conn == nil || blocksize != 1024*1024 || myerr != syserror.NoErr {
		t.Fatal("ERROR: Cannot connect to the server")
	}

	senderr := fscomm.SendMsg(conn, fscomm.TERMMSG, nil)
	if senderr != syserror.NoErr {
		t.Fatal("Cannot send termination message")
	}

	// Message successfully sent, we poll for the server termination and let things happen
	for {
		if IsServerDone() == 1 {
			break
		}
	}

}

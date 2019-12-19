/*
 * Copyright (c) 2018-2019 Geoffroy Vallee, All rights reserved
 * Copyright (c) 2019 Sylabs, Inc. All rights reserved
 * This software is licensed under a 3-clause BSD license. Please consult the
 * LICENSE.md file distributed with the sources of this project regarding your
 * rights to use or distribute this software.
 */

package metadataserver

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/gvallee/syserror/pkg/syserror"
	"github.com/sylabs/SyADFS/internal/pkg/dataserver"
)

func createVirtualServer(t *testing.T) (*Dataserver, syserror.SysError) {
	server := new(Dataserver)
	serverURL := "127.0.0.1:5552"
	myerr := server.Init(serverURL)
	if myerr != syserror.NoErr {
		t.Fatal("cannot add virtual server")
	}
	server.SetBlocksize(512)
	datasvrBasedir, err := ioutil.TempDir("", "datasvr-")
	if err != nil {
		t.Fatalf("failed to create temporary directory: %s", err)
	}
	dataserver := dataserver.Init(datasvrBasedir, 512, serverURL)
	if dataserver == nil {
		t.Fatal("unable to create dataserver")
	}

	return server, syserror.NoErr
}

func TestGeneric(t *testing.T) {
	basedir, err := ioutil.TempDir("", "metadata-")
	if err != nil {
		t.Fatalf("failed to create FS' base directory: %s", err)
	}
	defer os.RemoveAll(basedir)

	/* Create data servers */
	server1, syserr1 := createVirtualServer(t)
	if syserr1 != syserror.NoErr {
		t.Fatal("FATAL: Cannot create a virtual server")
	}
	serverURL, parseerr := server1.GetURL()
	if parseerr != syserror.NoErr {
		t.Fatal("FATAL: Cannot get server's URI")
	}

	/* Initialize the client (it will initialize the metadata server) */
	myFS := ClientInit(basedir, serverURL)
	if myFS == nil {
		t.Fatal("FATAL: Cannot initialize the client")
	}

	/* Prepare the buffer to write */
	buff1 := make([]byte, 1024)

	/* Actually doing a write operation */
	ws, writeErr := myFS.Write("default", buff1)
	if writeErr != syserror.NoErr {
		t.Fatal("write failed: ", writeErr.Error())
	}
	if ws != 1024 {
		t.Fatal("written size is invalid")
	}

	moreData := make([]byte, 124)

	for i := 0; i < 3; i++ {
		ws, writeErr = myFS.Write("default", moreData)
		if writeErr != syserror.NoErr {
			t.Fatal("FATAL, write failed: ", writeErr.Error())
		}
		if ws != 124 {
			t.Fatal("FATAL: written size is invalid")
		}
	}
}

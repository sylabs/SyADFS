/*
 * Copyright (c) 2018-2019 Geoffroy Vallee, All rights reserved
 * Copyright (c) 2019 Sylabs, Inc. All rights reserved
 * This software is licensed under a 3-clause BSD license. Please consult the
 * LICENSE.md file distributed with the sources of this project regarding your
 * rights to use or distribute this software.
 */

// THIS IS NOT REALLY A METADATA SERVER BUT RATHER AN EXAMPLE BECAUSE THE METADATA SERVER
// IS ATM FUSED INTO THE APPLICATION.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gvallee/syserror/pkg/syserror"
	ms "github.com/sylabs/SyADFS/internal/pkg/metadataserver"
)

func main() {
	/* Argument parsing */
	basedir := flag.String("basedir", "", "Data server base directory")
	servers := flag.String("servers", "", "Data servers to connect to")

	flag.Parse()

	/* We check whether the basedir is valid or not */
	_, myerror := os.Stat(*basedir)
	if myerror != nil {
		log.Fatal(myerror)
	}
	fmt.Println("Basedir:", *basedir)

	/* Initialize the client side of the file system */
	myFS := ms.ClientInit(*basedir, *servers)
	if myFS == nil {
		log.Fatal("ERROR: ClientInit() failed")
	}

	/* Prepare the buffer to write */
	fmt.Println("Writing 2000 bytes...")
	buff1 := make([]byte, 2000)
	if buff1 == nil {
		log.Fatal("ERROR: Cannot allocate buffer")
	}
	// We fill up the buffer with '3' so we can check the content after a read
	var val int = 3
	for i := 0; i < len(buff1); i++ {
		buff1[i] = byte(val)
	}

	ws, writeerr := myFS.Write("default", buff1)
	if ws != 2000 || writeerr != syserror.NoErr {
		log.Fatal("ERROR: Write op failed")
	}

	fmt.Println("Writing 500 bytes...")
	buff3 := make([]byte, 500)
	if buff3 == nil {
		log.Fatal("ERROR: Cannot allocate buffer")
	}
	// We fill up the buffer with '7' so we can check the content after a read
	val = 7
	for i := 0; i < len(buff3); i++ {
		buff3[i] = byte(val)
	}

	ws, writeerr = myFS.Write("default", buff3)
	if ws != 500 || writeerr != syserror.NoErr {
		log.Fatal("ERROR: Write op failed")
	}

	fmt.Println("Reading 1200 bytes...")
	buff2, readerr := myFS.Read("default", 1200)
	if readerr != syserror.NoErr || uint64(len(buff2)) != 1200 {
		log.Fatal("ERROR: Read operation failed")
	}

	// We check the content of the buffer
	for i := 0; i < len(buff2); i++ {
		value := int(buff2[i])
		if value != 3 {
			log.Fatal("ERROR: read ", value, " instead of 3 at index ", i)
		}
	}

	fmt.Println("Reading 1300 bytes...")
	buff4, readerr := myFS.Read("default", 1300)
	if readerr != syserror.NoErr || uint64(len(buff4)) != 1300 {
		log.Fatal("ERROR: Read operation failed")
	}

	// We check the content of the buffer
	var i int = 0
	for i = 0; i < 800; i++ {
		value := int(buff4[i])
		if value != 3 {
			log.Fatal("ERROR: read ", value, " instead of 3 at", i)
		}
	}
	for ; i < 1300; i++ {
		value := int(buff4[i])
		if value != 7 {
			log.Fatal("ERROR: read ", value, " instead of 7 at", i)
		}
	}

	/* End the client side of the file system */
	myFS.ClientFini()

	// We do not have a termination handshake so we wait a little to give a chance to the data servers to terminate
	fmt.Println("Terminating, please wait...")
	time.Sleep(5 * time.Second)
}

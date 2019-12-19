/*
 * Copyright (c) 2018-2019 Geoffroy Vallee, All rights reserved
 * Copyright (c) 2019 Sylabs, Inc. All rights reserved
 * This software is licensed under a 3-clause BSD license. Please consult the
 * LICENSE.md file distributed with the sources of this project regarding your
 * rights to use or distribute this software.
 */

package dataserver

import (
	"log"
	"os"
	"strconv"

	"github.com/gvallee/syserror/pkg/syserror"
)

/**
 * NamespaceInit initializes a namespace. The function can safely be called multiple times. If the
 * namespace already exists, the function simply returns successfully.
 * @param[in]   name    Namespace's name
 * @param[in]   ds      Structure representing the server
 * @return      Namespace handle
 */
func NamespaceInit(name string, dataserver *Server) *Namespace {
	namespacePath, myerr := GetBasedir(dataserver)
	if myerr != syserror.NoErr {
		log.Println(myerr.Error())
		return nil
	}
	namespacePath += "/"
	namespacePath += name
	_, myerror := os.Stat(namespacePath)
	if myerror != nil {
		// The path does not exist
		myerror := os.MkdirAll(namespacePath, 0700)
		if myerror != nil {
			log.Fatal(myerror)
			return nil
		}
	}

	new_namespace := new(Namespace)
	new_namespace.path = namespacePath
	return new_namespace
}

/**
 * Get the path to the file where the block is saved. The underlying file will be correctly
 * opened/created.
 * @param[in]   ds      Structure representing the server
 * @param[in]   namespace       Namespace's namespace we want to write to
 * @param[in]   blockid         Block id to write to
 * @return      File handle that can be used for write operations
 */
func getBlockPath(dataserver *Server, namespace string, blockid uint64) (*os.File, string, syserror.SysError) {
	block_file, myerr := GetBasedir(dataserver)
	if myerr != syserror.NoErr {
		log.Println(myerr.Error())
		return nil, "", myerr
	}
	block_file += namespace + "/block"
	block_file += strconv.FormatUint(blockid, 10)

	/*
		 _, mystaterror := os.Stat (block_file)
		 // Perform the actual write operation
		 var f *os.File
		 var myerror error
		 if (mystaterror != nil) {
				 // The file does not exist yet
		 log.Println ("Creating block file")
				 f, myerror = os.Create (block_file)
		 } else {
				 // The file exists, we open it
		 log.Println ("Opening block file")
				 f, myerror = os.OpenFile (block_filei, os.O_RDWR|os.O_CREATE, 0755))
		 }
	*/
	f, myerror := os.OpenFile(block_file, os.O_RDWR|os.O_CREATE, 0755)
	if myerror != nil {
		log.Println(myerror.Error())
		return nil, "", syserror.ErrNotAvailable
	}

	return f, block_file, syserror.NoErr
}

/**
 * BlockWrite writes a data to a block
 * @param[in]   ds      Structure representing the server
 * @param[in]   namespace       Namespace's namespace we want to write to
 * @param[in]   blockid         Block id to write to
 * @param[in]   offset          Write offset
 * @param[in]   data            Buffer with the data to write to the block
 * @return      Amount of data written to the block in bytes
 * @return      System error handle
 */
func BlockWrite(dataserver *Server, namespace string, blockid uint64, offset uint64, data []byte) (int, syserror.SysError) {
	// Making sure that the data to write fits into the block
	blocksize, dserr := GetBlocksize(dataserver)
	if dserr != syserror.NoErr {
		log.Println(dserr.Error())
		return -1, dserr
	}
	if offset+uint64(len(data)) > blocksize {
		log.Println("Data overflow - Write", len(data), "from", offset, "while blocksize is", blocksize)
		return -1, syserror.ErrDataOverflow
	}

	// Figure out where to write the data
	f, _, myerr := getBlockPath(dataserver, namespace, blockid)
	defer f.Close()
	if myerr != syserror.NoErr {
		return -1, myerr
	}

	// Actually write the data
	log.Println("Actually writing", len(data), "bytes to block", blockid, ", starting at", offset)
	s, mywriteerr := f.WriteAt(data, int64(offset)) // Unfortunately, Write return an INT
	if mywriteerr != nil {
		log.Println(mywriteerr.Error())
		return -1, syserror.ErrFatal
	}
	f.Sync()

	// All done
	return s, syserror.NoErr
}

/**
 * BlockRead reads a data block
 * @param[in]   ds      Structure representing the server
 * @param[in]   namespace       Namespace's namespace we want to write to
 * @param[in]   blockid         Block id to write to
 * @param[in]   offset          Write offset
 * @param[in]   size            Amount of data to read
 * @return      Amount of data written to the block in bytes
 * @return      Buffer with the data read from the block
 * @return      System error handle
 */
func BlockRead(dataserver *Server, namespace string, blockid uint64, offset uint64, size uint64) (int, []byte, syserror.SysError) {
	blocksize, dserr := GetBlocksize(dataserver)
	if dserr != syserror.NoErr {
		return -1, nil, syserror.ErrFatal
	}
	if offset+size > blocksize {
		return -1, nil, syserror.ErrDataOverflow
	}

	// Figure out from where to read the data
	f, _, myerr := getBlockPath(dataserver, namespace, blockid)
	defer f.Close()
	if myerr != syserror.NoErr {
		log.Println(myerr.Error())
		return -1, nil, myerr
	}

	// Actually read the data
	buff := make([]byte, size)
	log.Println("Actually reading", size, " bytes from block", blockid, ", starting at", offset)
	s, myreaderr := f.ReadAt(buff, int64(offset)) // Unfortunately Read return an INT
	if myreaderr != nil {
		log.Println("ERRROR: Cannot read from file")
		return -1, nil, syserror.ErrFatal
	}

	// All done
	return s, buff, syserror.NoErr
}

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
	"net"
	"os"

	"github.com/gvallee/syserror/pkg/syserror"
	"github.com/sylabs/SyADFS/internal/pkg/fscomm"
)

type Server struct {
	basedir    string
	block_size uint64
	info       *fscomm.ServerInfo
	conn       net.Conn
}

type Namespace struct {
	path string
}

var server_done int = 0

/* Functions specific to the implementation of servers */

func IsServerDone() int {
	return server_done
}

func runCommServer(server *Server) syserror.SysError {
	var errorStatus syserror.SysError = syserror.NoErr
	info := server.info

	mycommerr := fscomm.CreateServer(info)
	if mycommerr != syserror.NoErr {
		log.Println("error creating comm server")
		return mycommerr
	}

	var commerr syserror.SysError
	server.conn, commerr = fscomm.GetConnFromInfo(info)
	if server.conn == nil || commerr != syserror.NoErr {
		return commerr
	}

	fscomm.HandleHandshake(server.conn)
	for server_done != 1 {
		msghdr, syserr := fscomm.GetHeader(server.conn)
		if syserr != syserror.NoErr {
			log.Println("ERROR: Cannot get header")
			return syserror.ErrFatal
		}

		if msghdr == fscomm.TERMMSG {
			server_done = 1
		} else if msghdr == fscomm.DATAMSG {
			log.Println("Handling data message")
			// Recv the length of the namespace
			nslen, syserr := fscomm.RecvUint64(server.conn)
			if syserr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}

			// Recv the namespace
			namespace, nserr := fscomm.RecvNamespace(server.conn, nslen)
			if nserr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}

			// Recv blockid
			blockid, berr := fscomm.RecvUint64(server.conn)
			if berr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}

			// Recv offset
			offset, oerr := fscomm.RecvUint64(server.conn)
			if oerr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}

			// Recv data size
			size, serr := fscomm.RecvUint64(server.conn)
			if serr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}

			// Recv the actual data
			data, derr := fscomm.DoRecvData(server.conn, size)
			if derr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}

			// Actually save the data
			_, we := BlockWrite(server, namespace, blockid, offset, data)
			if we != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}
		} else if msghdr == fscomm.READREQ {
			log.Println("Recv'd a READREQ")
			namespace, blockid, offset, size, recverr := fscomm.HandleReadReq(server.conn)
			if recverr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}

			log.Println("Reading block...", blockid, offset, size)
			// Upon reception of a read req, we get the data and send it back
			rs, buff, readerr := BlockRead(server, namespace, blockid, offset, size)
			if uint64(rs) != size || readerr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}

			log.Println("Sending read data...")
			senderr := fscomm.SendMsg(server.conn, fscomm.RDREPLY, buff)
			if senderr != syserror.NoErr {
				server_done = 1
				errorStatus = syserror.ErrFatal
			}
		} else {
			log.Println("Unexpected message, terminating: ", msghdr)
			errorStatus = syserror.ErrFatal
		}

		if server_done == 1 {
			log.Println("All done:", errorStatus.Error())
		}
	}

	log.Println("Finalizing server...")
	fscomm.FiniServer()

	return errorStatus
}

/**
 * Init initializes the data server
 * @param[in]	basedir	Path to the basedir directory that the server must use
 * @param[in]	block_size	Cannonical size of a block
 * @return	Pointer to a new Server structure; nil if error
 */
func Init(basedir string, block_size uint64, server_url string) *Server {
	// Deal with the server's basedir (we have to make sure it exists)
	_, myerror := os.Stat(basedir)
	if myerror != nil {
		return nil
	}

	// Check whether the block size is valid
	if block_size == 0 {
		return nil
	}

	// Create and return the data structure for the new server
	new_server := new(Server)
	new_server.basedir = basedir
	new_server.block_size = block_size
	new_server.info = fscomm.CreateServerInfo(server_url, block_size, 60)

	// Initialize the default namespace
	mydefaultnamespace := NamespaceInit("default", new_server) // Always use the default namespace by default
	if mydefaultnamespace == nil {
		log.Println("Cannot initialized the default namespace")
		return nil
	}

	go runCommServer(new_server)

	return new_server
}

// Fini cleanly finalizes a running server
func Fini() {
	// TODO close all file descriptors associated to blocks
}

/**
 * GetBasedir returns the basedir of the data server
 * @param[in]	ds	Structure representing the server
 * @return	String specifying the server's basedir path
 * @return	System error handle
 */
func GetBasedir(ds *Server) (string, syserror.SysError) {
	if ds == nil {
		return syserror.ErrNotAvailable.Error(), syserror.ErrNotAvailable
	}
	return ds.basedir, syserror.NoErr
}

/**
 * GetBlocksize returns the block size of the data server.
 * This is a server level parameter, not a namespace level parameters, at least not
 * at the moment.
 * @param[in]	ds	Structure representing the server
 * @return	Size of the block
 * @return	System error handle
 */
func GetBlocksize(ds *Server) (uint64, syserror.SysError) {
	if ds == nil {
		return 0, syserror.ErrNotAvailable
	}

	return ds.block_size, syserror.NoErr
}

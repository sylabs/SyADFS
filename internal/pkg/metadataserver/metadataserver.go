/*
 * Copyright (c) 2018-2019 Geoffroy Vallee, All rights reserved
 * Copyright (c) 2019 Sylabs, Inc. All rights reserved
 * This software is licensed under a 3-clause BSD license. Please consult the
 * LICENSE.md file distributed with the sources of this project regarding your
 * rights to use or distribute this software.
 */
package metadataserver

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"

	"github.com/gvallee/syserror/pkg/syserror"
	"github.com/sylabs/SyADFS/internal/pkg/fscomm"
)

/**
 * Dataserver is the structure representing the data server for a given namespace.
 * Really note that it is for a given namespace so if the server is used
 * for multiple namespace we will have as many Dataserver structures as
 * namespace
 */
type Dataserver struct {
	surl           string   // URL of the server, also used to connect to the server
	block_size     uint64   // block size specific to the server
	curWriteBlock  uint64   // current block used for write operations
	curWriteOffset uint64   // current block offset used for write operations
	curReadBlock   uint64   // current block used for read operations
	curReadOffset  uint64   // current block offset used for read operations
	conn           net.Conn // Connection to reach the server
}

/**
 * DataInfo is the structure that tracks the data saved in our file system based on offsets
 */
type DataInfo struct {
	start       uint64      // Absolute start offset
	end         uint64      // Absolute end offset
	server      *Dataserver // Where the data is stored
	blockid     uint64
	blockoffset uint64
}

/**
 * Server is the structure representing a meta-data server.
 */
type Server struct {
	basedir string // Basedir of the server (where metadata will be stored
}

/**
 * Namespace is the structure representing a namespace from a client/metadata server point of view.
 */
type Namespace struct {
	// Should store the state of the namespace
	name string   // namespace's name
	file *os.File // file handle that is used to store metadata in files (based on the meta-data server's basedir

	// Used to track where the data is
	datainfo []*DataInfo // Used to track where the data is

	// Cache data to track what data servers are used
	nds             int                    // List of data servers
	dataservers     map[string]*Dataserver // Map used to store information about data servers we are connecting to. quick lookup based on URI
	listDataservers []*Dataserver          // Global list of data server, used for lookups that are not based on the server's URL

	// We cache some information so we can easily resume/continue writing operations
	lastWriteDataserver *Dataserver // Pointer to the server where the last write operation was performed (used to continue write operations in sub-sequent operations)
	lastWriteBlockid    uint64      // Block id used for the last write operation
	writeOffset         uint64      // Block offset used for the last write operation

	// We cache some information so we can easily resume/continue reading operations
	lastReadDataserver *Dataserver // Pointer to the server where the last read operation was performed (used to continue read operations in sub-sequent operations)
	lastReadBlockid    uint64      // Block id used for the last read operation
	readOffset         uint64      // Block offset used for the last read operation
	globalOffset       uint64      // Global read offset, i.e., based on the overall data, not a specific block. Used for sequential reads over data spaning multiple servers.
}

/**
 * SyADFS is the structure used to store the state of our file system
 */
type SyADFS struct {
	namespaces     map[string]*Namespace // List of existing namespaces in the current FS; used to lookup a namespace
	listNamespaces []*Namespace          // Overall list of namespaces so we can iterate over them

	LocalMetadataServer *Server // Pointer to the meta-data server's structure.
}

/**
 * AddDataserver adds a server while doing manually conenct
 * or testing with virtual data servers.
 * @param[in]	namespace	Namespace's name for which the server needs to be added
 * @param[in]	ds		Pointer to the sttructure representing the server to be added
 * @param[in]	url		URL of the server to be added
 * @param[in]	blocksize	Dataserver's block size
 * @return	System error handle
 */
func (aFS *SyADFS) AddDataserver(namespace string, ds *Dataserver, url string, blocksize uint64) syserror.SysError {
	if aFS == nil {
		return syserror.ErrFatal
	}

	ns, nserr := aFS.LookupNamespace(namespace)
	if nserr != syserror.NoErr {
		return syserror.NoErr
	}

	ns.dataservers[url] = ds
	ns.listDataservers = append(ns.listDataservers, ds)
	ns.nds++
	ds.block_size = blocksize
	log.Printf("New dataserver (%s) registered with block size of %d\n", url, blocksize)

	return syserror.NoErr
}

/**
 * GetURL returns the URL for a specific data server
 * @return Pointer to a URL structure and system error handle
 */
func (ds *Dataserver) GetURL() (string, syserror.SysError) {
	if ds == nil {
		return "", syserror.ErrFatal
	}

	return ds.surl, syserror.NoErr
}

/**
 * GetFreeBlock returns the blockid where we have some free space to store data in
 * the context of a given namespace. Basically, when we know which data server to
 * use to store the data, this function let us in which block we must save the data
 * @param[in] ns	Current namespace for the operation
 * @return	ID of a free block that can be used for the current operation
 * @return	System error handle
 */
func (ds *Dataserver) GetFreeBlock(ns *Namespace) (uint64, syserror.SysError) {
	if ns == nil {
		return uint64(0), syserror.ErrFatal
	}

	if len(ns.datainfo) == 0 {
		// No data at all yet, we can use the first block
		return 0, syserror.NoErr
	}

	// We need to find the last block writen to that server for the current namespace
	var targetDS *Dataserver = nil
	for i := len(ns.datainfo) - 1; i >= 0; i-- {
		if ns.datainfo[i].server == ds {
			// We found it!
			targetDS = ns.datainfo[i].server
		}
	}

	if targetDS != nil {
		if targetDS.curWriteOffset < targetDS.block_size {
			// We still have space in the last block we used
			return targetDS.curWriteBlock, syserror.NoErr
		}

		// We need to use a brand new block
		return targetDS.curWriteBlock + 1, syserror.NoErr
	} else {
		// That server is not used yet so we start at the first block
		return 0, syserror.NoErr
	}
}

/**
 * Init initializes the internal representation of a data server. Note that we are still in the context of
 * a meta-data server, so this is only meta-information about existing data server; not the data server
 * itself.
 * @param[in]	url	Data server's URL
 * @return	System error handle
 */
func (ds *Dataserver) Init(url string) syserror.SysError {
	if ds == nil {
		return syserror.ErrNotAvailable
	}

	ds.surl = url
	ds.block_size = 0
	ds.curWriteBlock = 0
	ds.curWriteOffset = 0
	ds.curReadBlock = 0
	ds.curReadOffset = 0
	ds.conn = nil

	return syserror.NoErr
}

/**
 * Init initializes a namespace structure.
 * @param[in]	name	Namespace's name
 * @return	System error handle
 */
func (ns *Namespace) Init(name string) syserror.SysError {
	ns.name = name
	ns.file = nil

	// Initialize the variables used to cache information about write operations
	ns.lastWriteDataserver = nil
	ns.lastWriteBlockid = 0
	ns.writeOffset = 0

	// Initialize the variables used to cache information about read operations
	ns.lastReadDataserver = nil
	ns.lastReadBlockid = 0
	ns.readOffset = 0

	// Initialize the info we used for book keeping the data server
	// that are used in the context of the namespace
	ns.nds = 0
	ns.dataservers = make(map[string]*Dataserver)

	return syserror.NoErr
}

/**
 * GetBlocksize gets the block size of a specific data server. That information is set when
 * the meta-data server connects to a data server
 * @return	block size
 * @return	System error handle
 */
func (aServer *Dataserver) GetBlocksize() (uint64, syserror.SysError) {
	if aServer == nil {
		return 0, syserror.ErrFatal
	}

	return aServer.block_size, syserror.NoErr
}

/**
 * Explicitly set the block size of a specific data server.
 * @param[in]	size	Data server's block size
 * return	System error handle
 */
func (aServer *Dataserver) SetBlocksize(size uint64) syserror.SysError {
	if aServer == nil {
		return syserror.ErrNotAvailable
	}

	aServer.block_size = size
	return syserror.NoErr
}

/**
 * LookupNamespace looks up a namespace for a given file system
 * @param[in]	namespace	Namespace's name
 * @return	Pointer to the corresponding namespace's structure
 * @return	System error handle
 */
func (aGoFS *SyADFS) LookupNamespace(namespace string) (*Namespace, syserror.SysError) {
	return aGoFS.namespaces[namespace], syserror.NoErr
}

/**
 * LookupLastWriteBlockUsed looks up where the last write operation landed, i.e., which block on which server. Used to continue writing after
 * the last write operation.
 * @input[in]	namespace	Namespace's name used for the write operation
 * @return dataserver	Pointer to the structure representing the data server where the last write operation ended
 * @return blockid	Last block id used by the last write operation
 * @return blocksize	Last block size used by the last write operation
 * @return offset	Last block offset used by the last write operation
 * @return System error handle
 */
func (aGoFS *SyADFS) LookupLastWriteBlockUsed(namespace string) (*Dataserver, uint64, uint64, uint64, syserror.SysError) {
	if aGoFS == nil {
		return nil, 0, 0, 0, syserror.ErrFatal
	}

	ns, myerr := aGoFS.LookupNamespace(namespace)
	if myerr != syserror.NoErr || ns == nil {
		return nil, 0, 0, 0, syserror.ErrNotAvailable
	}

	if ns.lastWriteDataserver != nil {
		blocksize, myerr := ns.lastWriteDataserver.GetBlocksize()
		if myerr != syserror.NoErr || blocksize == 0 {
			return nil, 0, 0, 0, syserror.ErrFatal
		}

		return ns.lastWriteDataserver, ns.lastWriteBlockid, blocksize, ns.writeOffset, syserror.NoErr
	} else {
		// We could find a last write yet
		return nil, 0, 0, 0, syserror.NoErr
	}
}

/**
 * LookupLastReadBlockUsed looks up where the last read operation stopped, i.e., which block on which server. Used to continue read operations.
 * @input[in]   namespace       Namespace's name used for the read operation
 * @dataserver   Pointer to the structure representing the data server where the last read operation ended
 * @return blockid      Last block id used by the last read operation
 * @return blocksize    Last block size used by the last read operation
 * @return blockoffset  Last block offset used by the last read operation
 * @return globalOffset Global offset where the last read stopped
 * @return System error handle
 */
func (aGoFS *SyADFS) LookupLastReadBlockUsed(namespace string) (*Dataserver, uint64, uint64, uint64, uint64, syserror.SysError) {
	if aGoFS == nil {
		return nil, 0, 0, 0, 0, syserror.ErrFatal
	}

	ns, myerr := aGoFS.LookupNamespace(namespace)
	if myerr != syserror.NoErr || ns == nil {
		return nil, 0, 0, 0, 0, syserror.ErrNotAvailable
	}

	if ns.lastReadDataserver != nil {
		blocksize, myerr := ns.lastReadDataserver.GetBlocksize()
		if myerr != syserror.NoErr || blocksize == 0 {
			return nil, 0, 0, 0, 0, syserror.ErrFatal
		}

		return ns.lastReadDataserver, ns.lastReadBlockid, blocksize, ns.readOffset, ns.globalOffset, syserror.NoErr
	} else {
		// No read operation yet, we look from the data map where is the begining of the data in the namespace
		if ns.datainfo == nil || ns.datainfo[0].server == nil {
			// We try to do a read but we have no data
			return nil, 0, 0, 0, 0, syserror.ErrNotAvailable
		}
		firstserver := ns.datainfo[0].server
		blocksize, myerr := firstserver.GetBlocksize()
		if myerr != syserror.NoErr || blocksize == 0 {
			return nil, 0, 0, 0, 0, syserror.ErrFatal
		}

		return firstserver, 0, blocksize, 0, 0, syserror.NoErr
	}
}

/**
 * UpdateLastWriteInfo updates the information related to the last block access of the last write operation. Used to resume writting later on.
 * @param[in] ds	Pointer to the data server's structure used last.
 * @param[in] namespace	Namespace's name for the write operation
 * @param[in] blockid	Block id used last
 * @param[in] startOffset Offset in block used last
 * @param[in] writeSize	Amount of data writen last
 * @return	System error handle
 */
func (aGoFS *SyADFS) UpdateLastWriteInfo(ds *Dataserver, namespace string, blockid uint64, startOffset uint64, writeSize uint64) syserror.SysError {
	if aGoFS == nil {
		return syserror.ErrFatal
	}

	log.Println("Update last write info with block", blockid)
	ns, myerr := aGoFS.LookupNamespace(namespace)
	if myerr != syserror.NoErr {
		return syserror.ErrFatal
	}

	ns.lastWriteDataserver = ds
	ns.lastWriteBlockid = blockid
	ns.writeOffset = writeSize

	if blockid == ds.curWriteBlock && ds.curWriteOffset < ds.block_size {
		// The data was added to a block already inuse
		ds.curWriteOffset += writeSize
		if ds.curWriteOffset > ds.block_size {
			return syserror.ErrDataOverflow
		}
	} else {
		// We use a new block
		ds.curWriteOffset = writeSize
		ds.curWriteBlock = blockid
	}

	// Update the block map if necessary
	var entry_found int = 0
	start := blockid*ds.block_size + startOffset
	for i := 0; i < len(ns.datainfo); i++ {
		if ns.datainfo[i].start <= start && (ns.datainfo[i].start+ns.datainfo[i].server.block_size) > start {
			log.Println("Consecutive write detected to server", ds.surl, " with blocks ", ns.datainfo[i].blockid, "and", blockid)
			if ns.datainfo[i].blockid != blockid {
				// We are now using a new block
				break
			}

			if ns.datainfo[i].end != startOffset {
				// This is not a consecutive write, meaning since last write on this server, data was saved on another server so we need to precisely separate what is already there and the new data
				break
			}
			entry_found = 1
			// We added data to the block that we already used
			ns.datainfo[i].end += writeSize
		}
		if ns.datainfo[i].end > start {
			break
		}
	}

	// We do not have a structure to track data yet so we need to add a new one (the goal being to known exactly where all the data is)
	if entry_found == 0 {
		log.Println("Using a new block for", start, "to", start+writeSize, "on", ds.surl)
		di := new(DataInfo)
		di.start = start
		di.end = start + writeSize - 1
		di.server = ds
		di.blockid = blockid
		di.blockoffset = startOffset
		ns.datainfo = append(ns.datainfo, di)
	}

	log.Println("Recording write for namespace", namespace, "on block", blockid, "starting at", startOffset, "with", writeSize, "bytes on", ds.surl)

	// Flush the metadata to make sure it is saved on dick
	aGoFS.FlushMetadataToDisk(ns)

	return syserror.NoErr
}

/** UpdateLastReadInfo updates the metadata related to the last read operation.
 * @param[in] ds        Pointer to the data server's structure used last
 * @param[in] namespace Namespace's name for the read operation
 * @param[in] blockid   Block id used last
 * @param[in] startOffset Offset in block used last
 * @param[in] readSize Amount of data read last
 * @return      System error handle
 */
func (aGoFS *SyADFS) UpdateLastReadInfo(ds *Dataserver, namespace string, blockid uint64, readSize uint64) syserror.SysError {
	if aGoFS == nil {
		return syserror.ErrFatal
	}

	ns, myerr := aGoFS.LookupNamespace(namespace)
	if myerr != syserror.NoErr {
		return syserror.ErrFatal
	}

	ns.lastReadDataserver = ds
	ns.lastReadBlockid = blockid
	ns.readOffset = readSize
	ns.globalOffset = ns.globalOffset + readSize

	return syserror.NoErr
}

/**
 * Flush a specific namespace.
 * @param[in]	basedir	File system basedir
 * @return	System error handle
 */
func (ns *Namespace) flush(basedir string) syserror.SysError {
	if ns == nil {
		return syserror.ErrFatal
	}

	// Figure out the file specific to the namespace
	path := basedir + "/" + ns.name

	var myerr error

	// Make sure the file for saving metadata is correctly created and open
	if ns.file == nil {
		ns.file, myerr = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
		if myerr != nil {
			return syserror.ErrFatal
		}
	}

	// Write the metadata to the file
	_, seek_err := ns.file.Seek(0, 0)
	if seek_err != nil {
		return syserror.ErrFatal
	}
	_, write_err := ns.file.WriteString(ns.name + "\n")
	if write_err != nil {
		return syserror.ErrFatal
	}

	// Capture datainfo
	_, write_err = ns.file.WriteString(fmt.Sprintf("# data info\n"))
	if write_err != nil {
		return syserror.ErrFatal
	}
	_, write_err = ns.file.WriteString(fmt.Sprintf("%d\n", len(ns.datainfo)))
	if write_err != nil {
		return syserror.ErrFatal
	}
	for i := 0; i < len(ns.datainfo); i++ {
		_, write_err = ns.file.WriteString(ns.datainfo[i].server.surl + " ")
		if write_err != nil {
			return syserror.ErrFatal
		}
		_, write_err = ns.file.WriteString(fmt.Sprintf("%d ", ns.datainfo[i].start))
		if write_err != nil {
			return syserror.ErrFatal
		}
		_, write_err = ns.file.WriteString(fmt.Sprintf("%d\n", ns.datainfo[i].end))
		if write_err != nil {
			return syserror.ErrFatal
		}
	}

	// Capture dataservers' info
	_, write_err = ns.file.WriteString(fmt.Sprintf("# data servers' info\n"))
	if write_err != nil {
		return syserror.ErrFatal
	}
	_, write_err = ns.file.WriteString(fmt.Sprintf("%d\n", ns.nds))
	if write_err != nil {
		return syserror.ErrFatal
	}

	for i := 0; i < ns.nds; i++ {
		_, write_err = ns.file.WriteString(ns.listDataservers[i].surl + " ")
		if write_err != nil {
			return syserror.ErrFatal
		}
		_, write_err = ns.file.WriteString(fmt.Sprintf("%d ", ns.listDataservers[i].block_size))
		if write_err != nil {
			return syserror.ErrFatal
		}
		_, write_err = ns.file.WriteString(fmt.Sprintf("%d ", ns.listDataservers[i].curWriteBlock))
		if write_err != nil {
			return syserror.ErrFatal
		}
		_, write_err = ns.file.WriteString(fmt.Sprintf("%d\n", ns.listDataservers[i].curWriteOffset))
		if write_err != nil {
			return syserror.ErrFatal
		}
	}

	// Capture last write info
	_, write_err = ns.file.WriteString(fmt.Sprintf("# last write info\n"))
	if write_err != nil {
		return syserror.ErrFatal
	}
	if ns.lastWriteDataserver != nil {
		_, write_err = ns.file.WriteString(ns.lastWriteDataserver.surl + "\n")
		if write_err != nil {
			return syserror.ErrFatal
		}
	} else {
		_, write_err = ns.file.WriteString("None\n")
		if write_err != nil {
			return syserror.ErrFatal
		}
	}
	_, write_err = ns.file.WriteString(fmt.Sprintf("%d\n", ns.lastWriteBlockid))
	if write_err != nil {
		return syserror.ErrFatal
	}
	_, write_err = ns.file.WriteString(fmt.Sprintf("%d\n", ns.writeOffset))
	if write_err != nil {
		return syserror.ErrFatal
	}

	// Capture last read info
	_, write_err = ns.file.WriteString(fmt.Sprintf("# last read info\n"))
	if write_err != nil {
		return syserror.ErrFatal
	}
	if ns.lastReadDataserver != nil {
		_, write_err = ns.file.WriteString(ns.lastReadDataserver.surl + "\n")
		if write_err != nil {
			return syserror.ErrFatal
		}
	} else {
		_, write_err = ns.file.WriteString("None\n")
		if write_err != nil {
			return syserror.ErrFatal
		}
	}
	_, write_err = ns.file.WriteString(fmt.Sprintf("%d\n", ns.lastReadBlockid))
	if write_err != nil {
		return syserror.ErrFatal
	}
	_, write_err = ns.file.WriteString(fmt.Sprintf("%d\n", ns.readOffset))
	if write_err != nil {
		return syserror.ErrFatal
	}

	// Mark the end of the valid data
	_, write_err = ns.file.WriteString("# End of latest data")
	if write_err != nil {
		return syserror.ErrFatal
	}

	// Flush the file
	ns.file.Sync()

	return syserror.NoErr
}

/**
 * FlushMetadataToDisk flushes one of our file system's metadata to disk.
 * @param[in]	ns	Namespace to flush
 * @return	System error handle
 */
func (aGoFS *SyADFS) FlushMetadataToDisk(ns *Namespace) syserror.SysError {
	basedir := aGoFS.LocalMetadataServer.basedir

	go ns.flush(basedir) // flush can be expensive so we execute it asychronisely

	return syserror.NoErr
}

/**
 * AddNamespace adds a new namespace to an existing file system handle
 * @param[in] ns	Namespace to flush
 * @return	System error handle
 */
func (aGoFS *SyADFS) AddNamespace(ns *Namespace) syserror.SysError {
	if ns == nil {
		return syserror.ErrFatal
	}

	aGoFS.namespaces[ns.name] = ns
	aGoFS.listNamespaces = append(aGoFS.listNamespaces, ns)
	return syserror.NoErr
}

/**
 * ServerInit initializes a meta-data server.
 * @param[in]	basedir	Server's basedir
 * @return	Meta-data server's metadata
 */
func ServerInit(basedir string) *Server {
	// Deal with the server's basedir (we have to make sure it exists)
	_, err := os.Stat(basedir)
	if err != nil {
		log.Printf("[ERROR] base directory cannot be stat: %s", err)
		return nil
	}

	// Create and return the data structure for the new server
	new_server := new(Server)
	new_server.basedir = basedir
	return new_server
}

/**
 * Init initializes a new file system
 * @param[in]	basedir	File system's basedir
 * @return	System error handle
 */
func (aGoFS *SyADFS) Init(basedir string) syserror.SysError {
	// For now, the metadata server is embeded into the client
	aGoFS.LocalMetadataServer = ServerInit(basedir)
	if aGoFS.LocalMetadataServer == nil {
		log.Println("[ERROR] unable to initialize server initialization failed")
		return syserror.ErrFatal
	}
	aGoFS.namespaces = make(map[string]*Namespace)

	defaultNS := new(Namespace)
	defaultNS.Init("default")
	mysyserr := aGoFS.AddNamespace(defaultNS)
	if mysyserr != syserror.NoErr {
		log.Printf("[ERROR] unable to add namespace: %s", mysyserr.Error())
		return mysyserr
	}

	return syserror.NoErr
}

/**
 * ClientInit initializes the client side of our file system
 * @param[in]	metadata_basedir	Basedir that the metadata server will use (the metadata server is instantiated in the client)
 * @return	Pointer to the structure representing the new file system associated to the client
 */
func ClientInit(metadata_basedir string, servers string) *SyADFS {
	newGoFS := new(SyADFS)
	if newGoFS == nil {
		log.Println("unable to allocate new object")
		return nil
	}

	mysyserr := newGoFS.Init(metadata_basedir)
	if mysyserr != syserror.NoErr {
		log.Printf("[ERROR] unable to initialize file system: %s", mysyserr.Error())
		return nil
	}

	/* Connecting to data servers */
	list_servers := strings.Split(servers, " ")
	log.Println("Connecting to", len(list_servers), "servers: ", servers)

	for i := 0; i < len(list_servers); i++ {
		log.Println("\t-> Connecting to", list_servers[i])
		connerr := newGoFS.ConnectToDataserver(list_servers[i])
		if connerr != syserror.NoErr {
			return nil
		}
	}

	return newGoFS
}

/**
 * ClientFini finalizes a client.
 */
func (myfs *SyADFS) ClientFini() syserror.SysError {
	if myfs == nil {
		return syserror.NoErr
	} // FS is already finalized

	// We go through all the namespaces and disconnect from the data servers
	for i := 0; i < len(myfs.listNamespaces); i++ {
		for j := 0; j < len(myfs.listNamespaces[i].listDataservers); j++ {
			if myfs.listNamespaces[i].listDataservers[j] != nil && myfs.listNamespaces[i].listDataservers[j].conn != nil {
				// We assume that by the time we are done, we always disconnect from the data servers, sending them a termination message
				fscomm.SendMsg(myfs.listNamespaces[i].listDataservers[j].conn, fscomm.TERMMSG, nil)
				myfs.listNamespaces[i].listDataservers[j].conn.Close()
				myfs.listNamespaces[i].listDataservers[j].conn = nil
			}
		}
	}

	myfs = nil
	return syserror.NoErr
}

/**
 * ConnectToDataserver connects to an existing data server
 * @param[in]	server_url	URL of the server to conenct to
 * @return	System error handle
 */
func (myfs *SyADFS) ConnectToDataserver(server_url string) syserror.SysError {
	if myfs == nil {
		return syserror.ErrNotAvailable
	}

	log.Println("Connecting to dataserver", server_url)
	newDataServer := new(Dataserver)
	newDataServer.Init(server_url)

	conn, bs, connerr := fscomm.Connect2Server(server_url)
	newDataServer.conn = conn
	if connerr != syserror.NoErr {
		return syserror.ErrFatal
	}

	// We always assume that the default namespace is ready to go, it is our reference namespace. So we add the dataserver to the default namespace
	syserr := myfs.AddDataserver("default", newDataServer, server_url, bs)
	if syserr != syserror.NoErr {
		return syserror.ErrNotAvailable
	}

	return syserror.NoErr
}

/**
 * SendWriteReq sends a write request to a data server, so that the data will be stored in a block.
 * Everything is ready so that the server can save the data directly upon reception.
 * We assume the call does not block and is fault tolerant.
 * @parma[in]	dataserver	Target data server
 * @param[in]	namespace	Target namespace
 * @param[in]	blockid		Target block id on the data server
 * @param[in]	block_offset	Target block offset on the data server
 * @param[in]	buff		Data to write into the target block on the data server
 * @param[in]	buff_size	Size of the data in the buffer to save in the target block
 * @param[in]	buff_offset	Offset in the buffer to save in the target block
 * @return	System error handle
 */
func (myFS *SyADFS) SendWriteReq(dataserver *Dataserver, namespace string, blockid uint64, block_offset uint64, buff []byte, buff_size uint64, buff_offset uint64) syserror.SysError {
	// Get the connection for the server
	conn := dataserver.conn
	if conn == nil {
		log.Println("[ERROR] connection is undefined")
		return syserror.ErrNotAvailable
	}

	// We make sure to restrict the range on the data so we send just enough data for that
	// specific block.
	// Fortunately, Go slices are very helpful with notations such as buff[start:end]
	senderr := fscomm.SendData(conn, namespace, blockid, block_offset, buff[buff_offset:buff_offset+buff_size])
	if senderr != syserror.NoErr {
		return senderr
	}

	return syserror.NoErr
}

/**
 * SendReadReq sends a read request to a data server. The operation is blocking.
 * We assume the call does not block and is fault tolerant.
 * @parma[in]   dataserver      Target data server
 * @param[in]   namespace       Target namespace
 * @param[in]   blockid         Target block id on the data server
 * @param[in]   block_offset    Target block offset on the data server
 * @param[in]	size		Size of the data to read
 * @return      System error handle
 */
func (myFS *SyADFS) SendReadReq(dataserver *Dataserver, namespace string, blockid uint64, offset uint64, size uint64) syserror.SysError {
	// Get the connection for the server
	conn := dataserver.conn
	if conn == nil {
		return syserror.ErrNotAvailable
	}

	senderr := fscomm.SendReadReq(conn, namespace, blockid, offset, size)
	if senderr != syserror.NoErr {
		return senderr
	}

	return syserror.NoErr
}

/**
 * GetNextDataserver gets the new dataserver when the last block is full, or to start the very first write operation
 * @param[in]	ns	Namespace of the operation
 * @return	Pointer to the data server to use next
 * @return	System error handle.
 */
func (myFS *SyADFS) GetNextDataserver(ns *Namespace) (*Dataserver, syserror.SysError) {
	if myFS == nil {
		return nil, syserror.ErrFatal
	}

	serverID := rand.Intn(ns.nds)
	return ns.listDataservers[serverID], syserror.NoErr
}

/**
 * Write to an initialized file system.
 * Example:
 * s, err := myFS.Write ("namespace1", buff)
 * @param[in]	namespace	Namespace of the write operation
 * @param[in]	buff		Buffer to write
 * @return	Size in bytes written to the file system. Note that the operation is asynchronous, the data may still be in transit.
 * @return	System error handle
 */
func (myFS *SyADFS) Write(namespace string, buff []byte) (uint64, syserror.SysError) {
	// First we look up the namespace
	ns, mynserr := myFS.LookupNamespace(namespace)
	if mynserr != syserror.NoErr {
		log.Printf("[ERROR] namespace lookup failed: %s", mynserr.Error())
		return 0, syserror.ErrFatal
	}

	// Lookup where we wrote data (server + blockid)
	dataserver, blockid, blocksize, offset, mylookuperr := myFS.LookupLastWriteBlockUsed(namespace)
	if mylookuperr != syserror.NoErr {
		log.Printf("[ERROR] last write lookup failed: %s", mynserr.Error())
		return 0, mylookuperr
	}

	// A few variables that we will need, i.e., global info about the operation
	var writeSize uint64
	var totalSize uint64 = uint64(len(buff))
	var curSize uint64 = 0
	log.Println(totalSize, "bytes to write...")

	// Fill up the last block we used and split the rest of the data to different blocks on different dataserver
	if dataserver != nil && dataserver.curWriteOffset < dataserver.block_size {
		serverURL, mySrvLookupErr := dataserver.GetURL()
		if mySrvLookupErr != syserror.NoErr {
			return 0, syserror.ErrFatal
		}

		spaceLeft := blocksize - offset
		log.Println("Space left in block:", spaceLeft)
		if totalSize > spaceLeft {
			writeSize = spaceLeft
		} else {
			writeSize = totalSize
		}

		log.Println("Check A")
		log.Printf("Writing %d/%d to block %d on server %s\n", writeSize, len(buff), blockid, serverURL)
		sendWriteReqErr := myFS.SendWriteReq(dataserver, namespace, blockid, offset, buff, writeSize, curSize)
		log.Println("Check B")
		if sendWriteReqErr != syserror.NoErr {
			log.Printf("unable to send write request: %s", sendWriteReqErr.Error())
			return 0, syserror.ErrFatal
		}
		curSize += writeSize

		// Update the block map & last block used info
		update_err := myFS.UpdateLastWriteInfo(dataserver, namespace, blockid, dataserver.curWriteOffset, writeSize)
		if update_err != syserror.NoErr {
			log.Printf("unable to update last write info: %s", update_err.Error())
			return 0, update_err
		}

		log.Println("Wrote", curSize, "bytes so far")

		log.Println("****************")
	}

	// Get the next server where to write data
	for totalSize > curSize {
		nextDataserver, myQueryErr := myFS.GetNextDataserver(ns)
		if myQueryErr != syserror.NoErr {
			return 0, syserror.ErrNotAvailable
		}

		blocksize, myQueryErr = nextDataserver.GetBlocksize()
		if myQueryErr != syserror.NoErr {
			return 0, syserror.ErrFatal
		}

		serverURI, mySrvLookupErr := nextDataserver.GetURL()
		if mySrvLookupErr != syserror.NoErr {
			return 0, syserror.ErrFatal
		}

		freeBlockid, myblockidlookuperr := nextDataserver.GetFreeBlock(ns)
		if myblockidlookuperr != syserror.NoErr {
			return 0, myblockidlookuperr
		}

		writeSize = 0
		if blocksize > (totalSize - curSize) {
			writeSize = totalSize - curSize
		} else {
			writeSize = blocksize
		}

		log.Printf("Writing %d/%d to block %d on server %s\n", writeSize, len(buff), freeBlockid, serverURI)
		sendWriteReqErr := myFS.SendWriteReq(nextDataserver, namespace, freeBlockid, 0, buff, writeSize, curSize)
		if sendWriteReqErr != syserror.NoErr {
			log.Printf("unable to send write request: %s", sendWriteReqErr.Error())
			return 0, syserror.ErrFatal
		}

		curSize += writeSize

		// Update the block map & last block used info
		update_err := myFS.UpdateLastWriteInfo(nextDataserver, namespace, freeBlockid, 0, writeSize)
		if update_err != syserror.NoErr {
			log.Printf("unable to update last write info: %s", update_err.Error())
			return 0, update_err
		}

		log.Println("Wrote", curSize, "bytes so far")

		log.Println("****************")
	}

	return curSize, syserror.NoErr
}

/**
 * Read from an initialized file system
 * @param[in]	namespace	Namespace's name from wich we want to read
 * @param[in]	size		Size to read. Read operations are assumed to be serialized, in order, starting at the begining of the namespace
 * @return	Buffer with the received data
 * @return	System error handle
 * Example
 * s, buff, err := myFS.Read ("namespace2", size)
 */
func (myFS *SyADFS) Read(namespace string, size uint64) ([]byte, syserror.SysError) {
	// First we look up the namespace
	ns, mynserr := myFS.LookupNamespace(namespace)
	if mynserr != syserror.NoErr {
		return nil, syserror.ErrFatal
	}

	data := make([]byte, size)

	var totalReadSize uint64 = 0
	for totalReadSize < size {
		// Lookup where the data is (server + blockid)
		dataserver, blockid, _, _, global_offset, mylookuperr := myFS.LookupLastReadBlockUsed(namespace)
		if mylookuperr != syserror.NoErr {
			return nil, mylookuperr
		}

		// First thing to figure out: can we read more data from the last block we accessed?
		var targetDS *Dataserver = nil
		var di *DataInfo = nil
		for i := 0; i < len(ns.datainfo); i++ {
			if ns.datainfo[i].start <= global_offset && ns.datainfo[i].end > global_offset {
				targetDS = ns.datainfo[i].server
				di = ns.datainfo[i]
				log.Println("Found data starting at", global_offset, ", it is in block", blockid, "(", di.start, "-", di.end, ")")
				break
			}

			if global_offset == ns.datainfo[i].end {
				// We reached the end of the last block we read, so we look for the next chunk
				targetDS = ns.datainfo[i+1].server
				di = ns.datainfo[i+1]
			}
		}

		// if not the first read, global_offset points right now to the last piece
		// we already read so we will strat reading right after that
		nextReadGlobalOffset := global_offset

		if targetDS != nil {
			// We found a server, we start with some sanity checks
			if dataserver != targetDS {
				log.Println("Inconsistent server bookkeeping")
				return nil, syserror.ErrFatal
			}

			// How much more data can we get from there?
			readSize := di.end - nextReadGlobalOffset + 1
			if readSize+totalReadSize > size {
				readSize = size - totalReadSize
			}

			// Get that data
			log.Println("Sending read req to server", dataserver.surl, ", blockid: ", di.blockid, "offset: ", di.blockoffset, ", for ", readSize, "bytes")
			senderr := myFS.SendReadReq(dataserver, namespace, di.blockid, di.blockoffset, readSize)
			if senderr != syserror.NoErr {
				return nil, senderr
			}
			msgtype, datasize, blockdata, recverr := fscomm.RecvMsg(dataserver.conn)
			if msgtype != fscomm.RDREPLY || datasize != readSize || blockdata == nil || recverr != syserror.NoErr {
				return nil, syserror.ErrFatal
			}
			log.Println("Received data for range", nextReadGlobalOffset, "to", nextReadGlobalOffset+readSize, "(", readSize, "bytes) from", dataserver.surl, ", blockid:", di.blockid, ", offset:", di.blockoffset)

			// Copy the received data to the target buffer
			log.Println("Copying data to target buffer")
			copy(data[totalReadSize:totalReadSize+readSize], blockdata)

			log.Println("Updating read info after reading", readSize, "byte")
			myFS.UpdateLastReadInfo(dataserver, namespace, di.blockid, readSize)

			totalReadSize += readSize
			log.Println("We read", totalReadSize, "bytes so far")
		} else {
			// Nothing to read
			return nil, syserror.NoErr
		}
	}
	log.Println("****************")

	return data, syserror.NoErr
}

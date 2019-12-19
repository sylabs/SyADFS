# Introduction

SyADFS is Sylabs' Application-level Distributed File System, i.e., an application
level non-POSIX compliant distributed file system. The basic assumptions are:
- the metadata server is instantiated in memory in the application,
- data is organized via namespace; a "default" namespace is always available,
- an application reads and writes to a namespace in a serialized  manner,
- read and write operations are independent, meaning they both have their current offset,
- metadata is currently not saved and restored between runs (lack of time to implement the feature), meaning that between runs, we always start read/write operations from scratch,
- the data server save data in blocks, the metadata server fragment the data to write to distribute the blocks amongst the data servers,
- data servers are selected randomly when a new block is needed,
- each data server can have a different block size,
- the metadata server keep track of all the data in a namespace to create a full map of the data to blocks on data servers.

# Execution

We will illustrate how to start 2 data servers and a test example that will write/read data.

1. Open 3 terminal
2. In two terminals, start the data servers:
	- Go to the data server code directory and make sure GOPATH is correctly set.
	- Create the directories /tmp/gofs/dataserver1 and /tmp/gofs/dataserver2, which will be used to store data blocks.
	- In the first terminal, execute ```./cmd/dataserver/dataserver -basedir /tmp/gofs/dataserver1/ -url 127.0.0.1:4321 -block-size=1024```
	- In the second terminal, execute ```./cmd/dataserver/dataserver -basedir /tmp/gofs/dataserver2/ -url 127.0.0.1:4334 -block-size=512```
3. In the last terminal, start the application/metadata server:
	- Go to the metadata server code directory and make sure GOPATH is correctly set.
	- Create the directory /tmp/gofs/metadata/
	- Execute: ```./main -basedir=/tmp/gofs/metadata/ --servers="127.0.0.1:4321 127.0.0.1:4334"```

The output will show how the data is split to the different data servers upon write operations and retrieved from servers during read operations.

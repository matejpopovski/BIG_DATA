# P3 (5% of grade): Large, Thread-Safe Tables

## Overview

In this project, you'll build a server that handles the uploading of
CSV files, storing their contents, and performing operations on the
data.  You should think of each CSV upload as containing a portion of
a larger table that grows with each upload.

The server will write two files for each uploaded CSV file: one in CSV
format and another in Parquet. Clients that we provide will
communicate with your server via RPC calls.

Learning objectives:
* Implement logic for uploading and processing CSV and Parquet files
* Perform computations like summing values from specific columns
* Manage concurrency with locking in a multi-threaded server

Before starting, please review the [general project directions](../projects.md).

## Clarifications/Corrections

* 07 Oct, 2024: `autograde.py` and `check_sub.py` uploaded. Grading details updated.
* 10 Oct, 2024: Corrections about data types.
* 10 Oct, 2024: Increased timeouts from some tests. Update `autograde.py`. This will _not_ negatively effect existing submissions.
* 11 Oct, 2024: Changed the warning text in `autograde.py`. `[Warning]` will only appear in `STDOUT`, not in `score.json`.

## Part 1: Communication

In this project, three client programs (upload.py, csvsum.py, and
parquetsum.py) will communicate with a server, server.py, via gRPC.
We provide the client programs.  Your job is to write a .proto to
generate a gRPC stub (used by our clients) and servicer class (that
you will inherit from in server.py).

Take a moment to look at code for the three client programs and answer
the following:

* what are the names of the imported gRPC modules?  This will determine what you name your .proto file.
* what methods are called on the stubs?  This will determine the RPC calls in your .proto
* what arguments are passed to the methods, and what values are extracted from the return values?  This will determine the fields in the messages in your .proto
* what port number do the clients use?  This will determine the port that the server should use.

Write a .proto file based on your above observations and run the
grpc_tools.protoc tool to generate stub code for our clients and
servicer code for your server.  All field types will be strings,
except `total` and `csv_data`, which should be `int64` and 
`bytes` respectively.

Now build the .proto on your VM.  Install the tools like this:

```sh
python3 -m venv venv
source venv/bin/activate
pip3 install grpcio==1.66.1 grpcio-tools==1.66.1 numpy==2.1.1 protobuf==5.27.2 pyarrow==17.0.0 setuptools==75.1.0
```

Then use `grpc_tools.protoc` to build your .proto file.

In your server, override the two RPC methods for the generated
servicer.  For now, you can just return messages with the error field
set to "TODO", leaving any other field unspecified.

If communication is working correctly so far, you should be able to
start a server and used a client to get back a "TODO" error message
via gRPC:

```
python3 -u server.py &> log.txt &
python3 upload.py simple.csv
# should see "TODO"
```

Create a Dockerfile to build an image that will also let you run your
server in a container.  It should be possible to build and run your
server like this:

```
docker build . -t p3
docker run -d -m 512m -p 127.0.0.1:5440:5440 p3
```

The client programs should then be able to communicate with the
Dockerized programs the same way they communicated with the server
outside of a container.

If you want to make code changes without rebuilding the image each
time, consider using a volume mount to make the latest version of
server.py on your VM replace the server.py in the file (if the server
is not at /server.py inside the container, modify accordingly):

```
docker run --rm -m 512m -p 127.0.0.1:5440:5440 -v ./server.py:/server.py p3
```

## Part 2: Upload

When your server receives an upload request with some CSV data, your
program should write the CSV to a new file somewhere.  You can decide
the name and location, but the server must remember the path to the
file (for example, you could add the path to some data structure, like a
list or dictionary).

Your server should similarly write the same data to a parquet file
somewhere, using pyarrow.

## Part 3: Column Sum

When your server receives a column summation request, it should loop
over all the data that has been uploaded, computing a sum for each
file, and returning a total sum.

For example, assume file1.csv and file2.csv contain this:

```
x,y,z
1,2,3
4,5,6
```

And this:

```
x,y
5,10
0,20
```

You should be able to upload the files and do sums as follows:

```
python3 upload.py file1.csv
python3 upload.py file2.csv
python3 csvsum.py x # should print 10
python3 csvsum.py z # should print 9
python3 csvsum.py w # should print 0
```

You can assume any column you sum over contains only integers, but
some files may lack certain columns (e.g., it is OK to sum over z
above, even though file2.csv doesn't have that column).

The only difference between `csvsum.py` and `parquetsum.py` is that
they will pass the format string to your gRPC method as "csv" or
"parquet", respectively.  Your server is expected to do the summing
over either the CSV or parquet files accordingly (not both).  Given
the CSVs and parquets contain the same data, running `csvsum.py x`
should produce the same number as `parquetsum.py x`, though there may
be a performance depending on which format is used.

Parquet is a column-oriented format, so all the data in a single file
should be adjacent on disk.  This means it should be possible to read
a column of data without reading the whole file.  See the `columns`
parameter here:
https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html

**Requirement:** when the server is asked to sum over the column of a
Parquet file, it should only read the data from that column, not other
columns.

**Note:** we will run your server with a 512-MB limit on RAM.  Any
individual files we upload will fit within that limit, but the total
size of the files uploaded will exceed that limit.  That's why your
server will have to do sums by reading the files (instead of just
keeping all table data in memory).

## Part 4: Locking

You don't need to explicitly create threads using Python calls because
gRPC will do it for you.  Set `max_workers` to 8 so that gRPC will
create 8 threads:

```
grpc.server(
        futures.ThreadPoolExecutor(max_workers=????),
        options=[("grpc.so_reuseport", 0)]
)
```

Now that your server has multiple threads, your code should hold a
lock (https://docs.python.org/3/library/threading.html#threading.Lock)
whenever accessing any shared data structures, including the list(s)
of files (or whatever data structure you used). Use a single global
lock for everything.  Ensure the lock is released properly, even when
there is an exception. Even if your chosen data structures provide any
guarantees related to thread-safe access, you must still hold the lock
when accessing them to gain practice protecting shared data.

**Requirement:** reading and writing files is a slow operation, so
your code must NOT hold the lock when doing file I/O.

## Grading

<!-- Details about the autograder are coming soon. -->

Copy `autograde.py` to your working directory 
then run `python3 -u autograde.py` to test your work.
This constitutes 75% of the total score. You can add `-v` flag to get a verbose output from the autograder.

If you want to manually test on a somewhat bigger dataset, run
`python3 bigdata.py`.  This generates 100 millions rows across 400
files and uploads them.  The "x" column only contains 1's, so you if
sum over it, you should get 100000000.

The other 25% of the total score will be graded by us.
Locking and performance-related details are hard to automatically
test, so here's a checklist of things we'll be looking for:

- are there 8 threads?
- is the lock held when shared data structures accessed?
- is the lock released when files are read or written?
- does the summation RPC use either parquets or CSVs based on the passed argument?
- when a parquet is read, is the needed column the only one that is read?

## Submission

You have some flexibility in how your organize your project
files. However, we need to be able to easily run your code.  In order
to be graded, please ensure to push anything necessary so that we'll
be able to run your client and server as follows:

```sh
git clone YOUR_REPO
cd YOUR_REPO

# copy in tester code and client programs...
python3 -m venv venv
source venv/bin/activate
pip3 install grpcio==1.66.1 grpcio-tools==1.66.1 numpy==2.1.1 protobuf==5.27.2 pyarrow==17.0.0 setuptools==75.1.0

# run server
docker build . -t p3
docker run -d -m 512m -p 127.0.0.1:5440:5440 p3

# run clients
python3 upload.py simple.csv
python3 csvsum.py x
python3 parquetsum.py x
```

Please do include the files built from the .proto.  Do NOT include the venv directory.

After pushing your code to the designated GitLab repository, 
you can also verify your submission. 
To do so, simply copy `check_sub.py` to your working directory and run 
the command `python3 check_sub.py`

# Jig Wire Protocol

The Jig wire protocol is purpose-designed to achieve a number of goals:

1. Versioned.
2. Language independent.
3. Efficient.
4. Simple to implement in a client.
5. Synchronous.

The format is binary. At present it is a combination of an ad-hoc binary
protocol along with Protobuf-format messages and responses for most
operations. However, row and schema messages are custom-designed to
exploit knowledge of the data to reduce message size.

## Caveats

Both this document and the protocol itself is a work in progress. 
Be suspicious of details such as data types.

## Basics

The Jig protocol is a simple request/response mechanism. Each request
represents some specific client action. Most messages represent Jig API-level
operations, though some are at a lower level (such as log in.) The client 
sends a request and waits for the response.

All numbers are big-endian. In fact, the numeric compression depends on
the big-endian representation.

## Session Initiation

* The client connects to the server and sends a HelloRequest message with version
information.
* The server sends a HelloResponse that includes server version and
protocol version to use for the session.

## Hello Request

* Int 16: Request ID: 0
* Int 32: Length: 4
* Int 16: Client API version
* Int 16: Lowest supported version

### Hello Response

* Int 16: Response ID: 0
* Int 32: Length: 4
* Int 16: Server version
* Int 16: Selected API version

The returned selected version is the largest version supported by both the
client and server.

If the server cannot support the client version, the server returns an
error response (see below) and closes the connection. Note that the error
response itself may evolve, so we may have to define an error response
specifically for the incompatible versions situation that won't change
across releases.

## Standard Request/Response Format

After session initiation, the client and server exchange messages using a
protocol based on a set of conventions. Requests and responses have a
standard header:

* Int 16: Request or Response type
* Int 32: Length
* Varies: body

Most messages have a single length/body pair. A length of zero indicates that no body
is included.

Some messages include a list of length/body pairs. A length of 0 indicates
end of message.

Whether a message returns no body, a single body, or a list of bodies is a static
property of the message type. 

### OK Response

Many messages need just a simple acknowledgement in the form of the OK Response:

* Response Type: 1
* Length: 0

### Error Response

In most cases, each request has an associated response. If an error occurs, the server
will substitute an error reponse:

* Response Type: 3
* Length: computed
* Body: ErrorResponse protobuf

## User Login

Log into the server is a separate step from session initiation. This keeps session
setup simple, and allows the client to query the server about the login methods
that the server supports.

* The client requests the list of login methods. (Optional)
* The server returns a list of method names.
* The client requests the set of login method parameters for a login method. (Optional)
* The server returns a list of parameter definitions.
* The cient logs in using a supported method, including needed parameters. (Required)
* The server resonds with a success message, or an error.
* If success, the client proceeds to issue queries. If failure, the client can repeat
the above steps.

Each message has a null body or an associated protobuf:

* List login methods: Request type 1
* ListLoginsResponse
* ListLoginPropertiesRequest
* ListLoginPropertiesResponse
* LoginRequest
* OK or Error response

## Queries

At present Drill does not support query parameters, so no messages exist for preparing
a query or passing parameters. Instead all that is needed is to issue a query and read
results. Results are one of four message types.

## Execute Statement

The Execute Statement request is for those statements that return no results or a
single results. For example: `ALTER SESSION`.

* ExecuteRequest
* Error response or SuccessResponse

The success response includes the number of rows affected (or would if Drill supported
that feature). Some Drill statements return a single row with a status. Such results
will be mapped into the success response (but that work remains to be done.)

## Execute Query

A query is a statement that returns 0 or more result sets, each with a schema and
one or more rows. (More work is needed here: a LIMIT 0 query may return a schema
but no rows.)

First, submit the query.

* QueryRequest
* OK or Error response

If submission is OK, read results until EOF:

* Request data request: 6 (null body)
* One of the four results types:
** No results Response: (TBD)
** Schmema Response: 7
** Results Response: 8
** EOF Response: 9

At any point the client can cancel the query by sending a Cancel request in
place of the results request.

### No Results Response

The no results response is experimental. It allows the client to specify a maximum
wait time for the next schema or set of results. If the time limit is exceeded, the
server returns a no results response. The client can cancel the query, or can
again poll the server. This feature is not yet implemented in the current code.

### Schema Response

The schema response message has two implementations at present; we're evaluating
which one to use. One is a custom-built message, the other the SchemaResponse
protobuf. More information will be added here once we decide which to use.

### Results Response

Query results represent the bulk of the data transferred over the Jig protocol.
The results response message uses four ad-hoc techniques to compress results:

* Returns multiple rows per message up to a specified maximum buffer size.
* Represents nulls as a single bit.
* Represents 4 and 8-bit numbers in a compact format depending on value.
* Uses a "duplicate" bit to compress out values that are identical from
one row to the next.

The numeric compression is a work in progress: several techniques are under
review.

The duplicate field bit exists, but the logic to do the duplicate checks is
not yet implemented.

The Results response is the only message (at present) that uses the repeated-body
model: each row is a separate body, prefixed by row length. A length of 0 indicates
the end of the set of rows. The client sets the maximum results length and so
has the option to stream results directly into a pre-allocated buffer of the maximum
size.

Row structure:

* Length
* Header: null bits and repeated bits
* Fields

The Header is an array of 2-bit pairs of (null, repeated) values. The pairs
are packed four-to-a byte. The length of the header is known: it is derived
from the number of fields in the current schema. (Current implementation uses
null bits for all fields, even those that are NOT NULL.)

The header bits support three states:

* null, -
* not-null, repeated
* not-null, not repeated

Note that null values are just null, there is no value in marking nulls as repeated
even if they are repeated.

Fields appear in schema order, except that null or repeated fields are omitted.
Field format depends on data type:

* int8, uint8: 1 byte
* int16, uint16: 2 bytes
* int32, uint32, int64, uint64: compressed numeric format (see code)
* string: length (compressed numeric) followed by UTF-8 characters without 0x00 terminator
* float: 4 bytes
* double: 8 bytes
* others: not yet fully implemented

Reading all fields puts the reader at the end of the record, ready to read the next
record length and thus repeat the above process.

## Session Termination

* The client sends a Goodbye request.
* The server sends a Goodbye response.
* The client and server both close their connections.

Messages both have a null body:

* Goodbye request: 10
* Goodbye response: 10

## State Machines

The message structure is designed to allow the client and server to maintain state
machines that track session state. In
general, each request or response may trigger a well-defined state transition
within the state machine. See the implemetation classes for details. 
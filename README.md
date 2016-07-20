# Jig: A Light-Weight Client API for Drill

The Jig project provides a light-weight, versioned, row-oriented API on top of Apache Drill.

See the [Project Proposal](https://docs.google.com/document/d/1wzNZne5RrkN3MyOFNAsnNuSGrK4Am2OfZJXg2KfJXCY)
for an overview.

## Jig Modules

The Jig project divides into a number of modules:

- `jig-core`: The Jig API plus code common to the Jig client and server.
- `jig-client`: The library used by applications that use the Jig remote API.
- `jig-server`: The implementation of the Jig server (the "Drillpress".)
- `jig-jdbc`: A JDBC driver that sits on top of the Jig client library.
- `jig-extras`: Examples, tools and other tidbits outside of the above four packages.

## Jig Architecture

Jig provides three main components:

- A row-based API
- An implementation of the API on top of the Drill Client API. This is the "direct" mode: the
application that uses the Jig client runs in the same process as the Drill client itself.
- A remote implementation of the API in which the application runs in a process (and often host)
different than the one that runs the Drill client. The Drill client is housed in the "DrillPress"
server. An RPC API connects the client and the server.

The Drill Client API exposes Drill's internal Value Vector columnar data representation. Yet, most
applications require data presented as rows. The Jig API implementation on top of Drill "transposes"
the columnar value vectors into row-orientated iterators. The client API itself is a typical
database-like API with connections, statements, result sets and rows (called "tuples" in Jig.)

The unique aspects of the Jig API are:

- Jig provides a two-level iterator: the "result collection" iterator returns groups of rows in which
all rows in the group have the same schema; but different groups have different schemas (a "schema
change" in Drill terminology.) Each group consists of a schema and a tuple set, where each tuple
is a collection of fields. Applications can access tuple fields in any order, but tuples and result
sets are read sequentially.
- Jig provides the user set of scalar values, plus additional interfaces to access structured
fields (maps and lists.)

An application written against the Jig API can run either in direct model (Drill client in the same process)
or remote mode (Drill client in a separate process) with no code changes except for the connection factory.

For testing, the DrillPress server can run in-process (like an embedded Drillbit.) Indeed, a test program may
choose to run the application, the DrillPress and an embedded Drillbit all in the same process.

## API Versioning

Jig is designed to allow clients and servers to upgrade on separate schedules: older Jig clients can
work with newer Jig servers and visa-versa. Because the Drill RPC protocol is not versioned, the Jig
server must be at the same version as Drill itself. The Jig server then isolates clients from the
need to upgrade at the same time as the server.

## User Documentation

Extensive user-level documentation is available in the jig-distro module under src/assemble/doc.
# Jig Limitations

This early version of Jig is complete enough to play with, but has many limitations.

See the JDBC page for the JDBC limitations.

## Data Types

Please see the data types page for a detailed list of the supported and
unsupported data types.

## Java Only

The Jig wire protocol is designed to allow simple implemetations in other languages.
However, this release has only a Java implementation. Please let us know if you are 
interested in creating a client implementation in another language.

## Wire Protocol

An early goal is to get something working for comment. The Jig wire protocol uses
the Protostuff implementation of Google protobuf for many of its messages. Later
versions will likely change to use hand-written messages for two reasons:

1. Eliminates  external dependencies.
2. Easier implementation in other languages.

## Error Reporting

Jig is designed to map Drill errors into standard SqlCodes. The bulk of that work
will come later. (Drill's own errors are somewhat opaque, so work may be required in
Drill to give Jig the information needed to return clear error codes.)

## Performance

The Jig server introduces an additional "hop" in each client query. This will cause
some additional, but probably small, overhead. Performance testing has not yet been
done on Jig to quantify the cost.

## Drillpress (Jig Server)

The Jig server is best described as the simplest possible server implementation that
actually works. The server is a "target rich environment" for your suggestions. What
is needed to make the server work in your environment?

Internally, the Jig server uses Netty for server-side asynchrounous IO. Drill already
uses Netty in the Drill client, so using Netty for the Jig protocol is simple.
The current implementation needs an additional layer between Netty and the Drill
client. The current solution is good enough for a POC, however.

## Launch Scripts

Jig provides a script to launch the Drillpress server. These scripts depend on
script revisions merged into Drill 1.8. (Still pending as of this writing.)
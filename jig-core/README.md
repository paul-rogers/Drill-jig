# Jig Core Module

This package contains code shared by both the Jig client and server. Contents include:

- `jig.api`: The heart of the Jig project: the API that defines how to work with Drill
(or other) result sets.
- `jig.api.impl`: Common shell implementations of several API interfaces.
- `jig.proto`: Messages generated from Google protobuf definitions via the Protostuff project.
(These may be replaced with a differnt message form based on feedback.)
- `jig.protocol`: Hand-written message protocols used when initiating a connection.
- `jig.serde`: The row and schema serialization/deserialization implementations.

Because this is a core package, it has minimal dependencies, and depends
on no other Jig module.

## The Jig API

Key API interfaces include:

- `ConnectionFactory`: Creates a connection. Not strictly part of the API, but is instead provided
by each implementation of the API.
- `DrillConnection`: Connection to the Jig server.
- `Statement`: An executable statement that returns rows.
- `ResultCollection`: Unique to Jig: a collection of result sets, each with a distinct query. 
(A Drill schema change causes the creation of a new result collection.)
- `TupleSet`: A collection of rows, all with the same schema.
- `TupleSchema`: Metadata to describe the schema of a tuple set.
- `FieldAccessor`: Provides access to the value of a field as a scalar, an array or a map.

## Jig Wire Protocol

The Jig wire protocol is currently implemented using the Protostuff implementation of the
Google protobuf format. Future versions will likely switch to hand-written messages to
further reduce dependencies, because Jig messages are intended to be simple, and because
the Jig wire protocol iself is versioned.

## Tests

Tests depend on other Jig modules. To avoid complex dependencies, tests reside in the
jig-test module.

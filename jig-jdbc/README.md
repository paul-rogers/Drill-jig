# Jig JDBC Driver

The Jig JDBC driver provides (yet another) Drill JDBC implementation, this one based on the
Jig API. The driver uses the Jig remote client to connect to a Jig server, which then
connects to Drill.

The JDBC driver also supports an embedded Jig server and and embedded Drillbit, which is
useful for testing or small test applications.

User documentation is available in the jig-distro project in src/assemble/doc/Jig-JDBC.md.

## Architecture

The Jig JDBC driver is a composed of multiple layers of abstraction:

* The Jig JDBC driver provides the interface for applications.
* The Jig JDBC driver is a wrapper around the Jig remote implementation.
* The Jig remote implementation is a wrapper around the Jig wire protocol.
* The Jig wire protocol is implemented by the Jig server.
* The Jig server is a wrapper around the embeeded Jig server (Drillpress.)
* The Drillpress is a wrapper around the Jig direct API.
* The Jig direct API is a wrapper around the Drill Client API.
* The Drill Client API is a wrapper around the Drill RPC layer which connects to
the Drillbits that do the actual work.

All of this is transparent to users who simply provide a JDBC URL and move on to
the work at hand. The architecture is important, however, to anyone who wishes to
understand the code.

## Data Type Mapping

## Mapping Schema Changes to Result Sets

## Implementation Status

# Jig Client Module

This package contains code shared by both the Jig client and server. Contents include:

- `jig.client`: Remote client implementation of the Jig interfaces. This is the
programming API for code that chooses to use Jig directly.
- `jig.client.net`: Network client. Since Jig is synchronous, the cliet is quite
simple: it builds on blocking Java IO.


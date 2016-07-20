# The Jig JDBC Driver

Drill provides a complete JDBC implementation. Some distributions provide commercial JDBC
implementations. It goes without saying that the initial Jig version will not be as complete
as the existing solutions. Instead, the purpose in this release is simply to demonstrate
that a Jig version is ligher weight and provides schema-change support.

## JDBC Connection Strings

The Jig JDBC connection string is inspired by that of MySQL (link) and uses URL-like
syntax:

`jdbc:jig:method//user:password@host:port/schema/workspace?property=value&property=value...`

If you find you need to include a special URL character ("/:@?&=), just use the HTTP
"%" syntax: a percent sign followed by the two-digit UTF-8 (or ASCII) code for the
character.

### Connection Method

Jig JDBC sits atop the Jig API and so supports the same options as that API: remote
and direct. The direct case supports a remote or embedded Drillbit. The supported
methods are:

* (none): Remote client connecting to a Jig server.
* drill: Direct client connecting to a named remote Drillbit.
* zk: Direct client connecting to a remote Drillbit through a named ZooKeeper.
* config: Direct client connecting to a remote Drillbit using the ZooKeeper configured in
the Drill configuration file.
* embedded: Direct client connecting to an embedded Drillbit.

This seems like an odd collection of methods. All but the remote are simply wrappers
around the options exposed by the underlying Drill client.

### Credentials

You connect to the destination Drillbit using either "open" security (no user name
or password) or "basic" security (user name and password.) If a Drillbit is open,
it will just ignore any user name and password you may provide.

### Host and Port

The meaning of the host and port depends on the mode you selected:

* (none): Host and port of the Jig server. If omitted, host defaults to localhost,
and the port to the default Jig server port.
* drill: The host and port of the Drillbit.
* zk: The host and port of the ZooKeeper service. You may specify multiple hosts and
ports separted by commas.
* config: No host or port is necessary; they are obtained from the Drill config file.
* embedded: The host is always localhost. Specify the port on which the Drillbit
is configured to listen. If not provided, the value is taken from the Drill configuration.

### Schema

The optional initial schema (storage plugin and optional workspace) to use.

### Properties

Properties provide additional connection parameters as needed. Most of the above
settings have property equivalents:

* `method`
* `user`
* `password`
* `host`
* `port`
* `schema`
* `workspace`

This the following are equivalent:

`jdbc:jig//bob:secret@mybox:1234/dfs/orders`
`jdbc:jig?user=bob&password=secret&host=mybox&port=1234&schema=dfs&workspace=orders`

Some tools find it easier to generate the second form.

### Legacy Formats

Jig JDBC also recognizes the JDBC connection string format supported by the Apache Drill
JDBC driver.

## Supported Features

Jig JDBC supports most featues needed to connect to Drill, run a query and return results.
Supported features include:

* Schema changes (as multiple result sets)
* Result set metadata
* The simple Drill data types (Iteger, Float, Double, Varchar)

Jig JDBC has a number of limitations, either due to the nature of Drill or the Jig protocol:

* One active query per connection. (Though an application may have multiple connections open.)
* Only scroll forward cursors (no random access).
* No adds, deletes or updates.
* Query parameters are not supported.

Drill datatypes not yet supported include:

* (need list)

Many advanced JDBC operations are only partially supported, or not supported at all:

* Database metadata
* JDBC SQL syntax and placeholders.

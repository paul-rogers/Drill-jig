# Jig: A Light-Weight Client API for Apache Drill

(Copy)

Jig is a simple, light-weight client API that can either call Drill directly, or can use Jig's own wire protocol to talk
to a remote server. To undertand what this means for your application, let's discuss a bit about
how Drill works.

In a normal Drill setup, Drill is a distributed application: a set of Drillbits run on nodes of your
cluster. Like many "big data" tools, Drill uses ZooKeeper to coordinate Drillbits. One of the Drillbits
acts as the Foreman for a query session: the node to which a client application connects to run queries.

Drill uses a sophisticated, scalable, vector-based data representation and asynchronous I/O to maximize
throughput. Because Drill is a "schema on read" tool, Drill may find that the schema of data changes
across results (or, Drill's initial guess about the schema may turn out to require revision when later
data is read.)

Drill client applications use the Drill Client API which exposes the ZooKeeper coordination and the
vector-based results. Drill Clients must link in a large amount of Drill code, must work with ZooKeeper,
must work with asynchronous I/O, and must "rotate" results from a columnar to row-based view. Clients
must respond to "schema change" events while reading results.

## Jig Simplifies Your Application

While the Drill Client is a powerful API for high-volume applications, it can be
overkill for a typical client: it is a bit like connecting a 500 kilovolt power transmission line
directly to your house. What you really want is a "retail" version of Drill: something that provides
the basic query operations and presents results as a series of rows. Jig presents Drill schema
changes as a first-class API concept, making variable-schema data much easier to consume.

This, then, is the essense of the Jig concept: it is a "fixture that simplifies using your power tool."
Jig handles all the complexity of the Drill client so that you can get on with the task at hand:
submitting queries and processing the results.

As a first step, Jig can run as a API layer on top of the Drill client to provide a simplifed view of
of the Drill client API. In this mode, Jig supports both embedded and remote Drillbits. While this
is handy, we still have the need to link in large numbers of Drill jars and Drill's dependencies.

To overcome the dependency issue, Jig can also run in a client/server mode. The code that depends on
Drill runs in a Jig server, allowing the Jig client to be very light-weight. The Jig protocol is a
simple wire protocol that the client users to work with the Jig server.

The Jig server itself can run an embedded Drillbit (for very simple applications or for testing),
but more typically connects to a Drill cluster. When working with a cluster, the Jig server assumes
the tasks of working with ZooKeeper, including the task of selecting a Drillbit to act as Foreman for
each user connection.

Jig provides a JDBC wrapper around the Jig client; allowing any JDBC-aware application
to exploit Drill without the need to include Drill's many dependencies.

The Jig wire protocol is versioned, allowing newer Jig clients to work with older servers,
and visa-versa. Once Jig is mature, this will allow deployed applications to be upgraded on a
different schedule than the Drill servers, making Drill easier to manage with desktop applications.

The Jig wire protocol is delibrately simple in form and implementation to encourage the
community to create clients in other languages. The Jig project provides a Java implementation,
others can add a C, Python or other implementation. (The protocol is simple enough that it
can be implemented directly in Python, or the Python solution could be built atop a C
implementation.)

The Jig server is separate from the Drill server (the "Drillbit"). In part, this is a
pragmatic decision at this early project stage. But, it is also a strategic decision. Drillbits
run on a cluster. When run under YARN, the location of Drillbits can vary over time. Clients
use ZooKeeper to find the Drillbits. But, this creates a heavy burden for old-style desktop
clients. The Jig server can be run anywhere, including on servers outside the cluster. Jig
clients are configured to work with the well-known Jig server, while the Jig server assumes
the task of coordinating with Drillbits.

This design allows Jig servers to be load balanced, to use DNS to find the server(s) running
the Jig server, and so on. Such a model is closer to the classic desktop server design used
by databases, and so on. Clients get the power of Drill, but in a package that looks more like
a traditional database.

## Project Status

Jig is an early prototype: basic functionality works, but quite a bit of advanced functionality
remains to be added. The purpose is to get community suggestions about the concept: is this the
right approach; is the API as simple as possible; and so on. 

## Installation

Jig ships as an archive containing a collection of jars designed to be added to your existing
Drill application, or copied into your application project.

In all cases, start by unpacking the Jig archive:

`tar xzf drill-jig-0.0.1-SNAPSHOT.tar.gz`

This creates a directory called `drill-jig-0.0.1-SNAPSHOT` that contains a number of files,
including this one which means... you must have already done the above step... To make
subsequent steps easier, let's define `JIG_HOME`:

`export JIG_HOME=/path/to/drill-jig-0.0.1-SNAPSHOT`

## Drill Integration

Jig direct is the configuration in which the Jig API directly calls the Drill client in the
same Java process. The Jig server is simply an RPC layer on top of Jig direct.

To install Jig direct, copy jig into your `$DRILL_HOME` folder:

`cp $JIG_HOME $DRILL_HOME/jig`

## Jig Client

Jig remote is the configuration in which a client connects to Drill remotely using the
Jig wire protocol.

Jig is too immature to have its jars available on Maven. Instead, copy the jars from the
Jig home into your project:

`cp $JIG_HOME/client/*.jar /path/your/project`

The Jig client has no external dependencies, so the Jig jars are all you need. The client
includes the Jig client itself, and the Jig JDBC jar. (In this relese, the Jig JDBC jar
requires that the Jig client also be on the class path.)

## Configure the Jig Server

Configuring the Jig server is simple: you don't need to do anything by default. Jig reads
your existing Drill configuration files.

Additional configuration options are described later.

## Start the Jig Server

The Jig server runs much like Drill itself:

`$DRILL_HOME/jig/bin/jig-server.sh start [--site /path/to/site/dir]`

Stop the server as follows:

`$DRILL_HOME/jig/bin/jig-server.sh stop [--site /path/to/site/dir]`

The `--site` option is needed if your Drill site directory (configuration directory)
is outside of `$DRILL_HOME`.

## Test the Jig Server using Drill's sqlline Program

First, tell sqlline where it can find the Jig client jars:

`setenv DRILL_CLIENT_CLASS_PATH=$DRILL_HOME/jig/client/*`

Then, use the Jig JDBC option to connect:

`$DRILL_HOME/bin/sqlline jdbc:jig:///cp`

This says to connect to the Jig client on the local host using the default port, no login,
and select the class path (cp) data source.

If you have security enabled, have disabled cp, or started the Jig server on a different
host, use the more general form:

`$DRILL_HOME/bin/sqlline jdbc:jig://user:password@host/schema`

Where user, password are your credentials, and schema is your optional default storage
plugin. (Jig JDBC supports more connection options, discussed later.) If your password
contains an "@" symbol, use "%40" instead. Thus "C@t" becomes "C%40t".

Now, try a query. If you are using cp:

    SELECT * FROM `cp`.`employees` LIMIT 10

Congratulations, your Jig installation is up and running!

## Next Steps

From here, you can go in a number of directions:

1. Jig client example: client-example.md
2. Jig JDBC client example: jdbc-example.md
3. JDBC documentation: Jig-JDBC.md
4. Server configuration: server-config.md
5. Data type support: data-types.md
6. Limitations in This release: limitations.md

## Feedback

The purpose of this release is to encourage feedback. Please let us know your thoughts
on the Apache Drill dev mailing list: dev@drill.apache.org.

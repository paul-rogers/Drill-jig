# Jig Server Configuration

Jig server takes most of its configuration from the Drill configuration file.
However, the server allows additional configuration in a Jig-specific file:
`jig-server.conf` located in the `$DRILL_SITE/conf` directory (for many
Drill installations that location is `$DRILL_HOME/conf`.) The file uses
the same HOCON format (link) as Drill itself.

## Drill Integration

The Jig server scripts assume that they run from `$DRILL_HOME/jig/bin`.
The scripts use this location to find the Drill jars. If you use a site
directory outside of `$DRILL_HOME`, then you must pass that to the Jig
Server using the `--site` option as described in the section on starting
and stopping the server.

## Authentication Methods

The Jig API allows the client to query Drill to learn whether the Drillbit
requires authentication or not. At present, Drill provides no metadata with
this information. You can provide it as follows:

    drillpress.drill: {
      auth: "open" // or "basic"
    }
    
## Listen Port

The default Jig server listen port is ((need port)). You can change it:

    drillpress {
      port: 1234
    }

## Logging

(TBD)

## Multiple Jig Servers

You can run any number of Jig servers for the same Drill cluster. At present, each
Jig server can work with only one Drill cluster.

It should be possible to insert a load balancer in front of the Jig server as long as
the load balancer is session-aware: sends all traffic for each Jig session to the same
Jig server.

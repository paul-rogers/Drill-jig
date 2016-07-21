# Jig Server Module

The Drillpress (Jig server) hosts a Drill Client interface and an RPC server
that accepts connections from Jig clients. The Drillpress converts from
Drill's columnar, asynchronous interface to Jig's row-based syncronous
model.

The server can run anywhere on the network (not just cluster nodes) as
long as it has visibility to the ZooKeeper quorum and all Drillbits.

The command to launch the server is:

    drillpress.sh start --site <site dir>

Note that the `drillpress.sh` script requires the newer Drill scripts
merged into Drill 1.8 as part of the Drill-on-YARN project.

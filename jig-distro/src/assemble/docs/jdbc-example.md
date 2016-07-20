# Write a Simple Jig JDBC Program

Here we will develop a simple Jig JDBC program. Because you likely already know how to
use JDBC, we'll focus on the aspects unique to Jig: connection strings and multiple
result sets. (If you are not familiar with JDBC, you may wish to start with the Jig
client API, or consult one of the many JDBC introductions available in books and online.)

## Classic JDBC Program

We start with a simple program to run a query and print results.

((insert example))

## Schema Changes

Because Drill uses schema-on-read, the schema of the results may change over the course
of the query. Drill calls this a "schema change." However, JDBC does not have the concept
of schema change. It turns out that JDBC does have the ability to handle multiple result
sets from a single query, each of which may have its own schema. While multipe result sets
are uncommon from most SQL queries, they are sometimes used by stored procedures. So, we
just repurpose JDBC's stored procedure mechanisms to serve our needs.

((insert example))
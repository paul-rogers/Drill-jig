# Jig Extras

Code in this module are not part of Jig proper, but are handy for developing and testing
Jig. Only the jig-test module may depend on this module.

## JSON Support

It is often faster to test Jig using a data source other than Drill itself. One handy
tool is the ability to read JSON files. This is done using a combination of two modules.

'org.glassfish.json' is a customized version of the Glassfish JSON reader. Like many
JSON readers, the Glassfish version can read exactly one JSON object (array or list.)
But, most DB-style JSON files consist of a list of JSON objects. JSON purists will tell
you that such data should be recast as an array of objects. But, doing so forces the
JSON reader to read the entire file, since the reader wants to read the entire array
at once. The list-of-objects formats allows the reader to read each object one at a
time: something that scales better.

The Glassfish implementation is very close; only small changes were needed to allow
it to read multiple objects. We should eventually contribute the changes back to
Glassfish. The adapted implementation supports a subset of the Glassfish intialization
options; just enough to do what we need for testing.

The code is in the `org.glassfish.json` name space because it depends on protected
methods not available outside of that name space.

'...jig.api.json' is an implementation of the Jig API to read JSON files using the
modified Glassfish reader.

## Array Support

The JSON reader is great, but has a limitation: JSON has only two atomic types:
Number and String. Some Jig tests require the full range of Drill data types.
The Array reader implementation is a solution.

`...jig.api.array` is an implementation of the Jig API on top of a simple array-based
data set. To use it, simply set up the array with data values of any supported Java
type. The implementation will detect the types and map them to the corresponding Jig
types.

The implementation supports a list of arrays, each with its own schema, to test
schema chaange support.

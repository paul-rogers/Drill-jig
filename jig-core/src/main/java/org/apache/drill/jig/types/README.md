# Jig Type and Data Element System

This package implements Jig's type system, both the definitions and runtime aspects.

Jig supports the normal scalar data types (numbers, strings, etc.) Jig also supports
structured types: lists and maps, including lists of lists.

Although Jig is primarily designed to work with Drill, it does so in two ways:

1. By accessing Drill value vectors.
2. By accessing a serialized tuple buffer.

Jig uses the same API for both, but a different underlying implementation. Further,
the `jig-extras` project provides support to read test data from Java arrays and
from JSON files.

The layers of the Jig type & value system include:

* The tuple schema (`TupleSchema`) and field schema (`FieldSchems`) that provide
basic metadata.
* The tuple value (`TupleValue`) and field value (`FieldValue`) interfaces that
provide access to tuples (rows) and their fields (columns.) Scalar column values
are available directly from the `FieldValue`, lists and maps are available via
the `ArrayValue` and `MapValue` interfaces.
* A `FieldValueContainer` interface and its implementations that handle field
values that vary at runtime. For example, a nullable field can be either a
specific field value type (an int, say) or a null. A "variant" value can vary
between across all scalar values.
* The implementations of field values handle runtime value conversions. For example,
any number type can be converted to any other number type. Strings and numbers
can be converted between one another. Field value implementations handle these
conversions and are independent of the underlying data source.
* The `FieldAccessor` interface and its implementations provide a complete
system for accessing a variety of data sources.
* The underlying data source provides the actual data.

The above can be summarized in four layers:

1. Schema
2. Tuple & Field Values
3. Accessors
4. Data source

The schema and values comprise the Jig API. The data source is whatever Jig needs
to access. The accessors are the bridge between values and data source. In
addition, each implementation provides a mechanism to construct a Jig schema
from the data source, and to set up the value and accessor structure.

This package also provides a variety of `Conversions` classes that convert data
of a given type to other types. For example, the `Int32Conversions` provides
method to safely narrow `int` values. Trivial conversions (such as widening
an `int` to a `long`) are simply done inline whre needed.

Accessors are of two types. Most are implementation-specific and reside in the
package for that implementation. Others are generic and reside here. In particular,
this package contains generic Java accessors such as the `JavaMapAccessor` and
`CachedObjectAccessor`.

## Building a Schema

A data source implementation builds up both the schema (meta-data) and value
(data access) structures for each tuple set. (Recall that a tuple set represents
a group of tuples all with the same schema.) Build the schema and values
generally follows several steps:

1. Scan the underlying implementation to infer Jig data types for each
field. If the field is a list, infer the data type for list members as
well. (Maps are defined to have string keys and variant values.)
2. Determine the set of accessors needed to convert from implementation
representation to Jig representation.
3. Create a set of tuple and field values that map the accessors into
the declared Jig types.

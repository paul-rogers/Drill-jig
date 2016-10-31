# Schema-based JSON Reader Experiment

Drill provides a JSON reader that reads data from a JSON file and infers the
file schema from the actual data. There are times, however, when having a
schema can simplify the process of using Drill. For example:

* Empty files have no schema, yet some Drill operators require a schema
(even for "record batches" which are empty.) Some scanner substitute
a made-up schema, but that causes problems down-stream as described
below.
* Initial records in a file contain a run of null values, so that Drill
cannot immediately infer the field type. Once Drill does see the actual
type, a schema change occurs. Some Drill operators cannot handle a
schema change, complicating the process.
* Field may have inconsistent types that are artifacts of how the file
was written. A number may be quoted some times, but not other times.
An array may be omitted when it is empty or has a single value. And
so on.
* The meaning of a field may not be defined by JSON. For example,
date values are notoriously variable.
* Field names may not be ideal for users.

Some of the above issues can be resolved by views. But, views can
be expensive: they require that the data be read in one format then
later converted to another. Views also cannot handle issues such as
the array/scalar problem.

The mechanism here is an experiment to determine if the
[JSON Schema](http://json-schema.org) mechansm can be applied to Drill. 
Since working with
Drill data sources themselves are complex, this experiement works with
the simpler Jig mechanism as a prelude to working with Drill.

The schema definition is designed for eventual use in Drill and reflects
Drill semantics. Jig semantics are much simpler (because Jig is just
a presentation layer on top of Drill.) Certain schema properties are
unnecessary for Jig (such as nullability and fine-grain numeric types)
but are essential for good Drill performance on large data sets.

## Schema Definition

The schema definition is an extension of that defined in the
[Space Telescope JSON Schema Guide](https://spacetelescope.github.io/understanding-json-schema/).
Since the job of Drill is to map JSON into Drill types, the extensions
aid in that process.

Extensions include:

* Specify a specific numeric data type.
* Use the term "variant" for any scalar type.

Restrictions include:

* Only one type per field. (JSON Schema allows multiple.)

## Data Types

The JSON schema supports two types:

* The JSON Schema type which describes the JSON itself.
* The Jig type which states how the JSON value should be mapped into a Jig
type.

The JSON type is of one of two forms:

```
{ "type": "number" }              // Non-null number
{ "type": ["number", "string"] }  // String or number
{ "type": ["number", "null"] }    // Nullable number
```

Note that the Json type is assumed to be not nullable unless null appears
as one of the valid types.

Jig type is specified with an extension property:

```
{ "type": "number", jigType: "short" }
{ "type": ["number", "null"], jigType: "short" }
```

The above are variations on a theme of mapping a `number` to a `short`.
The jig type is assumed to be nullable if `null` appears as one of the
JSON types.

In addition, this system honors the `required` property. A required property must
be set to one of the declared types.

All Jig types are accepted in type definitions:

| Schema Type | Jig Type | JSON Type |
| ----------- | -------- | --------- |
| null | null | null |
| byte | int8 | number |
| short | int16 | number |
| integer | int32 | number |
| int | int32 | number |
| long | int64 | number |
| float | float32 | number |
| double | float64 | number |
| decimal | decimal | number |
| number | number | number |
| string | string | string |
| boolean | boolean | boolean |
| map | map | object |
| array | array | array |
| variant | variant | null, number, boolean, string |

Notes:

* number corresponds to any numeric type.
* variant corresponds to any non-structured type: number, string, boolean or null.

If no Jig type is provided then the following mapping is used:

| JSON Schema Type | Jig Type | Notes |
| ---------------- | -------- | ----- |
| null | null | field is always null or missing. |
| boolean | boolean | |
| number | number | Variant number |
| string | string | |
| object | map | |
| array | array | Same rules apply for element type |
| (none) | inferred | type is inferred by sampling data |

## Properties

The following properties are supported in the schema:

| Property | Description | Extension? |
| -------- | ----------- | ---------- |
| default  | Default field value | no |
| title    | Display name for field | no |
| description | Description of field | no |
| enum | Defined set of field values | no |
| required | Set of required fields | no |
| minLength | Minimum length of a string field | no |
| maxLength | Maximum length of a string field | no |
| format | Format of a string field | no |
| items | Identifies the type of array items | no |
| properties | Identifies the properties of an object | no |
| additionalProperties | Whether additional object properties are expected | no |
| range | The range of numeric values | no |

**default** provides a value for the field when the value is missing from
a record. (The default default is null.)

**title** provides a user-friendly display name. Not supported in Drill
at present, but should be the value returned from a `getDisplayName()`
method.

**description** provides a full-text description. Would be handy for use
in a system table query (though not supported in Drill at present.)

**required** standred way to identify a required set of fields,
though a field may be required but still null.

**enum** identifies the set of valid values for a field. Could be
used by the planner to determine field cardinality for use in filters
and joins.

**minLength**, **maxLength** describe the length range for a string field
and may be useful in query planning.

**format** provides a set of predefined field formats. Only the
`date-time` format adds information to Drill, allowing Drill to convert
the field to a date/time type.

**items** provides the data type for each item in an array. Also allows
specifying an array in "tuple" format (see the Guide.)

**properties** identifies the properties (name/value pairs) within a
JSON object. Top-level properties map to Drill fields. Those in nested
objects map to the items in the map.

**additionalProperties** identifies if a JSON map can have properties beyond
those that have been identified. If the value is `false`, the reader will
ignore any extra properties that may occur.

**range** defines the range for numeric values. The reader will choose
the smallest data type for the range.

## Syntax

The following summarizes the JSON Schema syntax, with extensions.

```
file -> root
root -> { definitions? value-body }
definitions -> "definitions" : { }
value-body -> type
              ("nullable" : boolean)?
type -> null-type | boolean-type | number-type | string-type | object-type | array-type
null-type -> "type" : "null"
boolean-type -> "type" : "boolean"
number-type -> "type" : "number" | "integer"
               ("minimum" : number)?
               ("maximum" : number)?
string-type -> "type" : "string"
               ("minLength" : number)?
               ("maxLength" : number)?
               ("format" : format-str)?
               ("enum" : [ string (, string)* ])?
object-type -> "type" : "object"
               properties?
               ("additionalProperties" : boolean )?
               ("required" : [ string (, string)* ])?
properties -> { property }
property -> name : { value-body }
array-type : "type" : "array" items?
items -> "items" : value-body | [ element (,element)* ]
         ("additionalItems" : boolean)?
element -> ("name" : string)? value-body
```     
package org.apache.drill.jig.api;

import org.apache.drill.jig.exception.JigException;

/**
 * Iterator over a collection of records all with the same schema.
 * <p>
 * Each tuple collection has the same record structure: nested
 * structures:<br>
 * { a: 1, b: "s", c: { d: 10, e: "t" }}<br>
 * Are either left as nested structures (as above), or flattened:<br>
 * { a: 1, b: "s", c.d: 10, c.e: "t" }<br>
 * <p>
 * In the nested case the top tuple has only the top level fields
 * (3 in the example), with one or more of those fields themselves
 * being tuples (field c above). Field indexes are relative to their
 * tuple.
 * <p>
 * In the more typical flattened case, all fields appear within the
 * top-level tuple, no nested tuples appear, field names of nested fields
 * are concatenated with the tuple name, and field indexes are with
 * the flattened structure (four fields in the above example.)
 * <p>
 * The flattened case is used for client/server messages. The nested
 * structure is available for implementing custom structures, such as
 * when working directly with JSON or as a wrapper around a flattened
 * structure.
 */

public interface TupleSet
{
  TupleSchema schema( );
  int getIndex( );
  boolean next( ) throws JigException;
  TupleValue tuple( );
}
package org.apache.drill.jig.api;

/**
 * Provides access to a single field. A field can be null
 * (no data), scalar (single value), a list, a structure, or
 * a map. The field accessor represents all types of fields.
 * Each field has one or more aspects, represented by a
 * specialized accessor. The aspect is null if the field
 * does not support that aspect. For scalar values, the
 * access methods throw an exception if the field is not
 * of the matching scalar type. The methods do not perform
 * conversions except in the case of extending a value from
 * a shorter to longer version of the same type.
 */

public interface FieldValue extends ScalarValue
{
  DataType type( );
  boolean isNull( );
  MapValue getMap( );
  ArrayValue getArray( );
  TupleValue getTuple( );
}
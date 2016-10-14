package org.apache.drill.jig.api;

/**
 * Access to an array of 0 or more items, each of which can
 * be any field type. The typical case is a list of values all
 * with the same schema, but this interface also handles the
 * general case in which the field type variable.
 */

public interface ArrayValue
{
  int size( );
  DataType memberType( );
  boolean memberIsNullable();
  FieldValue get( int i );
}

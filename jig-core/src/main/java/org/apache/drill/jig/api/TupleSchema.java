package org.apache.drill.jig.api;

/**
 * The schema of each record in a result collection. Also, the schema of
 * a nested structure within a record.
 */

public interface TupleSchema
{
  int getCount( );
  FieldSchema getField( int i );
  FieldSchema getField( String name );
}
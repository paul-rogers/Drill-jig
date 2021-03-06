package org.apache.drill.jig.api;

/**
 * Represents a run of one or more records that share a schema.
 * Similar to one of several result sets from a database stored procedure.
 * Each result collection represents a schema change event from Drill.
 */

public interface ResultCollection extends AutoCloseable
{
  int getIndex( );
  boolean next( ) throws JigException;
  TupleSet getTuples( );
  @Override
  void close( ) throws JigException;
}
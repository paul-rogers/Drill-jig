package org.apache.drill.jig.api;

/**
 * The schema of a single field.
 */

public interface FieldSchema
{
  String name( );
  int index();
  DataType type( );
  boolean nullable( );
  String getDisplayType();
  int getLength();
}
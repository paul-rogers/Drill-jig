package org.apache.drill.jig.api;

/**
 * The schema of a single field.
 */

public interface FieldSchema
{
  public static final String ELEMENT_NAME = "*";
  
  String name( );
  int index();
  DataType type( );
  boolean nullable( );
  FieldSchema member( );
  String getDisplayType();
  int getLength();
}
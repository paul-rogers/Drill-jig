package org.apache.drill.jig.extras.array;

/**
 * Defines a result set using plain old Java arrays. Fields can be (boxed)
 * primitives, Strings, BigDecimals. Use a List to represent a list field.
 * (Or, use a primitive array, or an array of objects.) Types are inferred
 * from the actual data. A field will be nullable if it actually contains
 * nulls, non-nullable otherwise. Use a Map instance to represent a map
 * field.
 */

public class Batch
{
  public String names[];
  public Object data[][];
  
  public Batch( String names[], Object data[][] ) {
    this.names = names;
    this.data = data;
  }
}
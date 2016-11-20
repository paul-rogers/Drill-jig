package org.apache.drill.jig.api;

/**
 * The schema of a single field.
 */

public interface FieldSchema
{
  public static final String ELEMENT_NAME = "*";

  /**
   * Name of the field. This is a simple name for most cases. It can be a
   * dotted name for flattened fields, of the form "a.b" where "a" is the
   * original (flattened away) map or tuple, and "b" is a field within that
   * map or tuple. Array elements also have a name, but the name is always
   * {@link #ELEMENT_NAME) (which is "*").
   * @return the field name
   */

  String name( );

  /**
   * The zero-based index of the field within the tuple that represents
   * a row.
   *
   * @return zero-based index of the field within its tuple
   */

  int index();

  /**
   * The Jig data type of the field.
   *
   * @return field data type
   */

  DataType type( );

  /**
   * Whether the field can contain nulls.
   * @return true if the field can contain nulls, false otherwise
   */

  boolean nullable( );

  /**
   * Returns the field definition for {@link DataType#LIST} fields.
   * @return the array element definition
   */

  FieldSchema element( );

  /**
   * Returns the tuple definition for {@link DataType#TUPLE} fields.
   * @return the tuple schema definition
   */

  TupleSchema schema( );
  String getDisplayType();
  int getLength();
}

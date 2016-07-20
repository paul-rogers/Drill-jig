package org.apache.drill.jig.api;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

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

public interface FieldAccessor
{
  public interface ScalarAccessor
  {
    boolean getBoolean( );
    byte getByte( );
    short getShort( );
    int getInt( );
    long getLong( );
    float getFloat( );
    double getDouble( );
    BigDecimal getDecimal();
    String getString( );
    byte[] getBlob( );
    LocalDate getDate( );
    LocalDateTime getDateTime( );
    Period getUTCTime( );
    Object getValue( );
  }
    
  public interface AnyAccessor extends ScalarAccessor
  {
    DataType getDataType( );
  }
    
  /**
   * Access to an array of 0 or more items, each of which can
   * be any field type. The typical case is a list of values all
   * with the same schema, but this interface also handles the
   * general case in which the field type variable.
   */
  
  public interface ArrayAccessor
  {
    int size( );
    DataType getValueType( );
    Cardinality getValueCardinality( );
    FieldAccessor get( int i );
    <V> V[] toArray( );
    int[] toIntArray( );
  }
  
  DataType getType( );
  Cardinality getCardinality( );
  boolean isNull( );
  FieldAccessor.ScalarAccessor asScalar( );
//    TupleAccessor asTuple( );
  FieldAccessor.ArrayAccessor asArray( );
//    MapAccessor asMap( );
  FieldAccessor.AnyAccessor asAny( );
}
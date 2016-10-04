package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.exception.ValueConversionError;

/**
 * Field value for a Null or Undefined field. Also used for any nullable
 * field type when the value is null. 
 */

public class NullFieldValue extends AbstractScalarFieldValue {
  
  public static final NullFieldValue INSTANCE = new NullFieldValue( );

  @Override
  public void bind(FieldAccessor accessor) {
  }

  @Override
  public DataType type() {
    return DataType.NULL;
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public boolean getBoolean() {
    return false;
  }

  @Override
  public byte getByte() {
    throw nullFieldError( );
  }

  @Override
  public short getShort() {
    throw nullFieldError( );
  }

  @Override
  public int getInt() {
    throw nullFieldError( );
  }

  @Override
  public long getLong() {
    throw nullFieldError( );
  }

  @Override
  public float getFloat() {
    throw nullFieldError( );
  }

  @Override
  public double getDouble() {
    throw nullFieldError( );
  }

  @Override
  public BigDecimal getDecimal() {
    throw nullFieldError( );
  }

  @Override
  public String getString() {
    throw nullFieldError( );
  }

  @Override
  public Object getValue() {
    return null;
  }
  
  private ValueConversionError nullFieldError( ) {
    return new ValueConversionError( "Cannot convert a null to a scalar type" );
  }
}

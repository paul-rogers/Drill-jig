package org.apache.drill.jig.api.impl;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldAccessor.AnyAccessor;
import org.apache.drill.jig.api.ValueConversionError;

public abstract class AbstractScalarAccessor implements AnyAccessor
{
  @Override
  public boolean getBoolean() {
    throw notSupportedError( DataType.BOOLEAN.getDisplayName() );
  }   

  @Override
  public byte getByte() {
    throw notSupportedError( DataType.INT8.getDisplayName() );
  }

  @Override
  public int getInt() {
    throw notSupportedError( DataType.INT32.getDisplayName( ) );
  }

  @Override
  public short getShort() {
    throw notSupportedError( DataType.INT16.getDisplayName() );
  }

  @Override
  public long getLong() {
    throw notSupportedError( DataType.INT64.getDisplayName() );
  }

  @Override
  public float getFloat() {
    throw notSupportedError( DataType.STRING.getDisplayName() );
  }

  @Override
  public double getDouble() {
    throw notSupportedError( DataType.FLOAT64.getDisplayName() );
  }

  @Override
  public BigDecimal getDecimal() {
    throw notSupportedError( DataType.DECIMAL.getDisplayName() );
  }

  @Override
  public String getString() {
    throw notSupportedError( DataType.STRING.getDisplayName() );
  }

  @Override
  public byte[] getBlob() {
    throw notSupportedError( DataType.BLOB.getDisplayName() );
  }

  @Override
  public LocalDate getDate() {
    throw notSupportedError( DataType.DATE.getDisplayName() );
  }

  @Override
  public LocalDateTime getDateTime() {
    throw notSupportedError( DataType.LOCAL_DATE_TIME.getDisplayName() );
  }

  @Override
  public Period getUTCTime() {
    throw notSupportedError( DataType.UTC_DATE_TIME.getDisplayName() );
  }

  @Override
  public Object getValue() {
    throw notSupportedError( "Object" );
  }
  
  public ValueConversionError notSupportedError(String type) {
    return new ValueConversionError( "Cannot convert " +
                getDataType( ).getDisplayName() +
                " to " + type );
  }
  
}

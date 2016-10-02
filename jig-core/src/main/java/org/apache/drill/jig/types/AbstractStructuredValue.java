package org.apache.drill.jig.types;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import org.apache.drill.jig.exception.ValueConversionError;

public abstract class AbstractStructuredValue implements AbstractFieldValue {

  @Override
  public boolean getBoolean() {
    throw typeError( );
  }

  @Override
  public byte getByte() {
    throw typeError( );
  }

  @Override
  public short getShort() {
    throw typeError( );
  }

  @Override
  public int getInt() {
    throw typeError( );
  }

  @Override
  public long getLong() {
    throw typeError( );
  }

  @Override
  public float getFloat() {
    throw typeError( );
  }

  @Override
  public double getDouble() {
    throw typeError( );
  }

  @Override
  public BigDecimal getDecimal() {
    throw typeError( );
  }

  @Override
  public String getString() {
    throw typeError( );
 }

  @Override
  public byte[] getBlob() {
    throw typeError( );
  }

  @Override
  public LocalDate getDate() {
    throw typeError( );
  }

  @Override
  public LocalDateTime getDateTime() {
    throw typeError( );
  }

  @Override
  public Period getUTCTime() {
    throw typeError( );
  }


  private ValueConversionError typeError( ) {
    return new ValueConversionError("Can't convert " + type( ) + " to a scalar");
  }
}

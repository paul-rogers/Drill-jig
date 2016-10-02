package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor.DecimalAccessor;

public class DecimalFieldValue extends AbstractScalarFieldValue {

  private DecimalAccessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (DecimalAccessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.DECIMAL;
  }

  @Override
  public boolean isNull() {
    return accessor.isNull();
  }

  @Override
  public boolean getBoolean() {
    return ! getDecimal( ).equals( BigDecimal.ZERO );
  }

  @Override
  public byte getByte() {
    return DecimalConversions.toByte( getDecimal( ) );
  }

  @Override
  public short getShort() {
    return DecimalConversions.toShort( getDecimal( ) );
  }

  @Override
  public int getInt() {
    return DecimalConversions.toInt( getDecimal( ) );
  }

  @Override
  public long getLong() {
    return DecimalConversions.toLong( getDecimal( ) );
  }

  @Override
  public float getFloat() {
    return DecimalConversions.toFloat( getDecimal( ) );
  }

  @Override
  public double getDouble() {
    return DecimalConversions.toDouble( getDecimal( ) );
  }

  @Override
  public BigDecimal getDecimal() {
    return accessor.getDecimal();
  }

  @Override
  public String getString() {
    return DecimalConversions.toString( getDecimal( ) );
  }

  @Override
  public Object getValue() {
    return getDecimal( );
  }

}

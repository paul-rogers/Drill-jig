package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor.Int64Accessor;

public class Int64FieldValue extends AbstractScalarFieldValue {

  private Int64Accessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (Int64Accessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.INT64;
  }

  @Override
  public boolean isNull() {
    return accessor.isNull();
  }

  @Override
  public boolean getBoolean() {
    return getLong() != 0;
  }

  @Override
  public byte getByte() {
    return Int64Conversions.toByte(getLong());
  }

  @Override
  public short getShort() {
    return Int64Conversions.toShort(getLong());
  }

  @Override
  public int getInt() {
    return Int64Conversions.toInt(getLong());
  }

  @Override
  public long getLong() {
    return accessor.getLong();
  }

  @Override
  public float getFloat() {
    return getLong();
  }

  @Override
  public double getDouble() {
    return getLong();
  }

  @Override
  public BigDecimal getDecimal() {
    return new BigDecimal(getLong());
  }

  @Override
  public String getString() {
    return Long.toString(getLong());
  }

  @Override
  public Object getValue() {
    return getLong();
  }

}

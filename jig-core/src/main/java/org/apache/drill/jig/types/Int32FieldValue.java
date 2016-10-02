package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor.Int32Accessor;

public class Int32FieldValue extends AbstractScalarFieldValue {

  private Int32Accessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (Int32Accessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.INT32;
  }

  @Override
  public boolean isNull() {
    return accessor.isNull();
  }

  @Override
  public boolean getBoolean() {
    return getInt() != 0;
  }

  @Override
  public byte getByte() {
    return Int32Conversions.toByte(getInt());
  }

  @Override
  public short getShort() {
    return Int32Conversions.toShort(getInt());
  }

  @Override
  public int getInt() {
    return accessor.getInt();
  }

  @Override
  public long getLong() {
    return getInt();
  }

  @Override
  public float getFloat() {
    return getInt();
  }

  @Override
  public double getDouble() {
    return getInt();
  }

  @Override
  public BigDecimal getDecimal() {
    return new BigDecimal(getInt());
  }

  @Override
  public String getString() {
    return Integer.toString(getInt());
  }

  @Override
  public Object getValue() {
    return getInt();
  }

}

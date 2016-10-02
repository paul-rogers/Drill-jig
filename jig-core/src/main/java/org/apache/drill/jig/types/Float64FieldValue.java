package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor.Float32Accessor;
import org.apache.drill.jig.types.FieldAccessor.Float64Accessor;

public class Float64FieldValue extends AbstractScalarFieldValue {

  private Float64Accessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (Float64Accessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.FLOAT32;
  }

  @Override
  public boolean isNull() {
    return accessor.isNull();
  }

  @Override
  public boolean getBoolean() {
    return getDouble() != 0;
  }

  @Override
  public byte getByte() {
    return Float64Conversions.toByte(getDouble());
  }

  @Override
  public short getShort() {
    return Float64Conversions.toShort(getDouble());
  }

  @Override
  public int getInt() {
    return Float64Conversions.toInt(getDouble());
  }

  @Override
  public long getLong() {
    return Float64Conversions.toLong(getDouble());
  }

  @Override
  public float getFloat() {
    return Float64Conversions.toFloat(getDouble());
  }

  @Override
  public double getDouble() {
    return accessor.getDouble();
  }

  @Override
  public BigDecimal getDecimal() {
    return new BigDecimal(getDouble());
  }

  @Override
  public String getString() {
    return Double.toString(getDouble());
  }

  @Override
  public Object getValue() {
    return getDouble();
  }

}

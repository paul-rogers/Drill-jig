package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.Float32Accessor;
import org.apache.drill.jig.api.DataType;

/**
 * Field value backed by a float value.
 */

public class Float32FieldValue extends AbstractScalarFieldValue {

  private Float32Accessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (Float32Accessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.FLOAT32;
  }

  @Override
  public boolean getBoolean() {
    return getFloat() != 0;
  }

  @Override
  public byte getByte() {
    return Float32Conversions.toByte(getFloat());
  }

  @Override
  public short getShort() {
    return Float32Conversions.toShort(getFloat());
  }

  @Override
  public int getInt() {
    return Float32Conversions.toInt(getFloat());
  }

  @Override
  public long getLong() {
    return Float32Conversions.toLong(getFloat());
  }

  @Override
  public float getFloat() {
    return accessor.getFloat();
  }

  @Override
  public double getDouble() {
    return getFloat();
  }

  @Override
  public BigDecimal getDecimal() {
    return new BigDecimal(getFloat());
  }

  @Override
  public String getString() {
    return Float.toString(getFloat());
  }

  @Override
  public Object getValue() {
    return getFloat();
  }

}

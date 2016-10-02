package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor.Int16Accessor;

/**
 * Field value backed by a short value.
 */

public class Int16FieldValue extends AbstractScalarFieldValue {

  private Int16Accessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (Int16Accessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.INT16;
  }

  @Override
  public boolean getBoolean() {
    return getShort() != 0;
  }

  @Override
  public byte getByte() {
    return Int16Conversions.toByte(getShort());
  }

  @Override
  public short getShort() {
    return accessor.getShort();
  }

  @Override
  public int getInt() {
    return getShort();
  }

  @Override
  public long getLong() {
    return getShort();
  }

  @Override
  public float getFloat() {
    return getShort();
  }

  @Override
  public double getDouble() {
    return getShort();
  }

  @Override
  public BigDecimal getDecimal() {
    return new BigDecimal(getShort());
  }

  @Override
  public String getString() {
    return Short.toString(getShort());
  }

  @Override
  public Object getValue() {
    return getShort();
  }

}

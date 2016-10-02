package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor.Int8Accessor;

/**
 * Field value backed by a byte value.
 */

public class Int8FieldValue extends AbstractScalarFieldValue {

  private Int8Accessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (Int8Accessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.INT8;
  }

  @Override
  public boolean getBoolean() {
    return getByte() != 0;
  }

  @Override
  public byte getByte() {
    return accessor.getByte();
  }

  @Override
  public short getShort() {
    return getByte();
  }

  @Override
  public int getInt() {
    return getByte();
  }

  @Override
  public long getLong() {
    return getByte();
  }

  @Override
  public float getFloat() {
    return getByte();
  }

  @Override
  public double getDouble() {
    return getByte();
  }

  @Override
  public BigDecimal getDecimal() {
    return new BigDecimal(getByte());
  }

  @Override
  public String getString() {
    return Byte.toString(getByte());
  }

  @Override
  public Object getValue() {
    return getByte();
  }

}

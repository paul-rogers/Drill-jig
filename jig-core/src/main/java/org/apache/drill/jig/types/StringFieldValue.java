package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.StringAccessor;
import org.apache.drill.jig.api.DataType;

/**
 * Field value backed by a String value.
 */

public class StringFieldValue extends AbstractScalarFieldValue {

  private StringAccessor accessor;

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (StringAccessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.STRING;
  }

  @Override
  public boolean getBoolean() {
    return StringConversions.toBoolean(getString());
  }

  @Override
  public byte getByte() {
    return StringConversions.toByte(getString());
  }

  @Override
  public short getShort() {
    return StringConversions.toShort(getString());
  }

  @Override
  public int getInt() {
    return StringConversions.toInt(getString());
  }

  @Override
  public long getLong() {
    return StringConversions.toLong(getString());
  }

  @Override
  public float getFloat() {
    return StringConversions.toFloat(getString());
  }

  @Override
  public double getDouble() {
    return StringConversions.toDouble(getString());
  }

  @Override
  public BigDecimal getDecimal() {
    return StringConversions.toDecimal(getString());
  }

  @Override
  public String getString() {
    return accessor.getString();
  }

  @Override
  public Object getValue() {
    return getString();
  }
}

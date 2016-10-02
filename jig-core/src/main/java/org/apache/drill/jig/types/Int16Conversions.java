package org.apache.drill.jig.types;

import org.apache.drill.jig.exception.ValueConversionError;

public class Int16Conversions {

  private Int16Conversions() { }

  public static byte toByte(short value) {
    if (value < Byte.MIN_VALUE || Byte.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: short to byte");
    return (byte) value;
  }

}

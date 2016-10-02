package org.apache.drill.jig.types;

import org.apache.drill.jig.exception.ValueConversionError;

public class Int32Conversions {

  private Int32Conversions() { }

  public static byte toByte(int value) {
    if (value < Byte.MIN_VALUE || Byte.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: int to byte");
    return (byte) value;
  }

  public static short toShort(int value) {
    if (value < Short.MIN_VALUE || Short.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: int to byte");
    return (short) value;
  }
}

package org.apache.drill.jig.types;

import org.apache.drill.jig.exception.ValueConversionError;

public class Int64Conversions {

  private Int64Conversions() { }

  public static byte toByte(long value) {
    if (value < Byte.MIN_VALUE || Byte.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: long to byte");
    return (byte) value;
  }

  public static short toShort(long value) {
    if (value < Short.MIN_VALUE || Short.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: long to byte");
    return (short) value;
  }

  public static int toInt(long value) {
    if (value < Integer.MIN_VALUE || Integer.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: long to int");
    return (int) value;
  }
}

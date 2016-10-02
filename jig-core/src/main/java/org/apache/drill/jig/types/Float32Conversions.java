package org.apache.drill.jig.types;

import org.apache.drill.jig.exception.ValueConversionError;

public class Float32Conversions {

  private Float32Conversions() { }

  public static byte toByte(float value) {
    return Int32Conversions.toByte(toInt(value));
  }

  public static short toShort(float value) {
    return Int32Conversions.toShort(toInt(value));
  }

  public static int toInt(float value) {
    if (value < Integer.MIN_VALUE || Integer.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: float to int");
    return Math.round(value);
  }

  public static long toLong(float value) {
    if (value < Long.MIN_VALUE || Long.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: float to int");
    return Math.round((double) value);
  }
}

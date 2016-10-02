package org.apache.drill.jig.types;

import org.apache.drill.jig.exception.ValueConversionError;

/**
 * Conversion functions from a double value.
 */

public class Float64Conversions {

  private Float64Conversions() { }

  public static byte toByte(double value) {
    return Int64Conversions.toByte(toLong(value));
  }

  public static short toShort(double value) {
    return Int64Conversions.toShort(toLong(value));
  }

  public static int toInt(double value) {
    return Int64Conversions.toInt(toLong(value));
  }

  public static long toLong(double value) {
    if (value < Long.MIN_VALUE || Long.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: float to int");
    return Math.round((double) value);
  }

  public static float toFloat(double value) {
    if (value < -Float.MAX_VALUE || Float.MAX_VALUE < value)
      throw new ValueConversionError("Value overflow: double to float");
    return (float) value;
  }
}

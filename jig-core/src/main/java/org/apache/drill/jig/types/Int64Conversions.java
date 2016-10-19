package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.exception.ValueConversionError;

/**
 * Conversion functions from a long value.
 */

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
  
  public static final BigDecimal TWO_TO_64 = new BigDecimal( Long.MAX_VALUE ).multiply( new BigDecimal( 2 ) );
  
  /**
   * Convert an unsigned int64, stored as a signed long, to a
   * BigDecimal value.
   * @param value
   * @return
   */
  
  public static BigDecimal unsignedToDecimal( long value ) {
    BigDecimal decimal = new BigDecimal( value );
    if ( value < 0 ) {
      decimal.add( TWO_TO_64 );
    }
    return decimal;
  }
}

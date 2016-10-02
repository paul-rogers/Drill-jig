package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.exception.ValueConversionError;

public class DecimalConversions {
  
  private DecimalConversions( ) { }

  public static byte toByte(BigDecimal value) {
    return Int32Conversions.toByte( toInt( value ) );
  }

  public static short toShort(BigDecimal value) {
    return Int32Conversions.toShort( toInt( value ) );
  }

  public static int toInt(BigDecimal value) {
    try {
      return value.toBigInteger().intValueExact();
    } catch (ArithmeticException e) {
      throw new ValueConversionError( "Overflow converting Decimal to int" );
    }
  }

  public static long toLong(BigDecimal value) {
    try {
      return value.toBigInteger().longValueExact();
    } catch (ArithmeticException e) {
      throw new ValueConversionError( "Overflow converting Decimal to long" );
    }
  }

  public static float toFloat(BigDecimal value) {
    return value.floatValue();
  }

  public static double toDouble(BigDecimal value) {
    return value.doubleValue();
  }

  public static String toString(BigDecimal value) {
    if ( value == null )
      return "null";
    else
      return value.toString();
  }

}

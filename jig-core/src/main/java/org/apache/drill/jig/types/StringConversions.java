package org.apache.drill.jig.types;

import java.math.BigDecimal;

import org.apache.drill.jig.exception.ValueConversionError;

public class StringConversions {

	public static boolean toBoolean(String value) {
		return Boolean.parseBoolean( value );
	}

	public static byte toByte(String value) {
		try {
			return Byte.parseByte( value );
		} catch (NumberFormatException e) {
			throw new ValueConversionError( "Conversion error: string to byte" );
		}
	}

	public static short toShort(String value) {
		try {
			return Short.parseShort( value );
		} catch (NumberFormatException e) {
			throw new ValueConversionError( "Conversion error: string to short" );
		}
	}

	public static int toInt(String value) {
		try {
			return Integer.parseInt( value );
		} catch (NumberFormatException e) {
			throw new ValueConversionError( "Conversion error: string to int" );
		}
	}

	public static long toLong(String value) {
		try {
			return Integer.parseInt( value );
		} catch (NumberFormatException e) {
			throw new ValueConversionError( "Conversion error: string to int" );
		}
	}

	public static float toFloat(String value) {
		try {
			return Float.parseFloat( value );
		} catch (NumberFormatException e) {
			throw new ValueConversionError( "Conversion error: string to float" );
		}
	}

	public static double toDouble(String value) {
		try {
			return Double.parseDouble( value );
		} catch (NumberFormatException e) {
			throw new ValueConversionError( "Conversion error: string to double" );
		}
	}

	public static BigDecimal toDecimal(String value) {
		try {
			return new BigDecimal( value );
		} catch (NumberFormatException e) {
			throw new ValueConversionError( "Conversion error: string to decimal" );
		}
	}
}

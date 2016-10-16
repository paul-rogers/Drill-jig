package org.apache.drill.jig.types;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.exception.ValueConversionError;

/**
 * Base class for scalar field values. Provides default do-nothing
 * Implementations for the non-scalar methods.
 */

public abstract class AbstractScalarFieldValue implements AbstractFieldValue {

  @Override
  public boolean isNull() {
    return false;
  }
  
  @Override
  public byte[] getBlob() {
    throw typeError("blob");
  }

  @Override
  public LocalDate getDate() {
    throw typeError("date");
  }

  @Override
  public LocalDateTime getDateTime() {
    throw typeError("local Date/Time");
  }

  @Override
  public Period getUTCTime() {
    throw typeError("UTC Time");
  }

  @Override
  public MapValue getMap() {
    throw typeError("map");
  }

  @Override
  public ArrayValue getArray() {
    throw typeError("array");
  }
  
  private ValueConversionError typeError(String dest) {
    return new ValueConversionError("Can't convert scalar to " + dest);
  }
  
  @Override
  public String toString( ) {
    StringBuilder buf = new StringBuilder( );
    buf.append( "[Field Value: null=" );
    buf.append( isNull( ) );
    buf.append( ", type=" );
    buf.append( type( ) );
    if ( ! isNull( ) ) {
      buf.append( ", value=" );
      Object value = getValue( );
      if ( value instanceof String ) {
        buf.append( "\"" );
      }
      buf.append( value );
      if ( value instanceof String ) {
        buf.append( "\"" );
      }
    }
    buf.append( "]" );
    return buf.toString( );
  }
}

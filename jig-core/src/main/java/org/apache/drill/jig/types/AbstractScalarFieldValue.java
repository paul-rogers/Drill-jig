package org.apache.drill.jig.types;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.util.JigUtilities;

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

  @Override
  public TupleValue getTuple() {
    throw typeError("tuple");
  }

  private ValueConversionError typeError(String dest) {
    return new ValueConversionError("Can't convert scalar to " + dest);
  }

  @Override
  public String toString( ) {
    StringBuilder buf = new StringBuilder( );
    JigUtilities.objectHeader( buf, this );
    buf.append( " null = " );
    buf.append( isNull( ) );
    buf.append( ", type = " );
    buf.append( type( ) );
    if ( ! isNull( ) ) {
      buf.append( ", value = " );
      JigUtilities.quote( buf, getValue( ) );
    }
    buf.append( "]" );
    return buf.toString( );
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( " null = " );
    buf.append( isNull( ) );
    buf.append( ", type = " );
    buf.append( type( ) );
    buf.append( "]" );
  }
}

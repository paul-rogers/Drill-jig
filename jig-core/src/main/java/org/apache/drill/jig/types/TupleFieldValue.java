package org.apache.drill.jig.types;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.TupleValueAccessor;
import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.util.JigUtilities;

public class TupleFieldValue extends AbstractStructuredValue implements TupleValueAccessor {

  protected TupleValueAccessor accessor;

  @Override
  public boolean isNull() {
    return accessor.isNull( );
  }

  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (TupleValueAccessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.TUPLE;
  }

  @Override
  public MapValue getMap() {
    throw new ValueConversionError( "Cannot convert a tuple to a map" );
  }

  @Override
  public ArrayValue getArray() {
    throw new ValueConversionError( "Cannot convert a tuple to an array" );
  }

  @Override
  public TupleValue getTuple() {
    return accessor.getTuple();
  }

  @Override
  public Object getValue() {
    throw new ValueConversionError( "Cannot convert a tuple to an object" );
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.visualizeLn(buf, indent + 1, "accessor", accessor);
    JigUtilities.indent(buf, indent + 1);
    buf.append( "]" );
  }
}

package org.apache.drill.jig.types;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.TupleValueAccessor;
import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.ValueConversionError;

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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getValue() {
    throw new ValueConversionError( "Cannot convert a tuple to an object" );
  }
}

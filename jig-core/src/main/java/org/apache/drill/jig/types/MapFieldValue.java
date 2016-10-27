package org.apache.drill.jig.types;

import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.ValueConversionError;

public class MapFieldValue extends AbstractStructuredValue {

  private MapValueAccessor accessor;
  
  @Override
  public void bind(FieldAccessor accessor) {
    this.accessor = (MapValueAccessor) accessor;
  }

  @Override
  public DataType type() {
    return DataType.MAP;
  }

  @Override
  public boolean isNull() {
    return accessor.isNull();
  }

  @Override
  public MapValue getMap() {
    return accessor.getMap();
  }

  @Override
  public ArrayValue getArray() {
    throw new ValueConversionError( "Cannot convert a map to a list" );
  }

  @Override
  public TupleValue getTuple() {
    throw new ValueConversionError( "Cannot convert a map to a tuple" );
  }

  @Override
  public Object getValue() {
    throw new ValueConversionError( "Cannot convert a map to an object" );
  }
}

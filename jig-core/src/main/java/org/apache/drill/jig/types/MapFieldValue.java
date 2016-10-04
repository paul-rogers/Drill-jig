package org.apache.drill.jig.types;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.types.FieldAccessor.MapValueAccessor;

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
  public Object getValue() {
    return accessor.getValue();
  }

}

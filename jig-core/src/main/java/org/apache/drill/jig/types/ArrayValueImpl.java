package org.apache.drill.jig.types;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;

public class ArrayValueImpl implements ArrayValue {

  private final DataType memberType;
  private final boolean memberIsNullable;
  private final FieldValueContainer container;
  private ArrayAccessor arrayAccessor;

  public ArrayValueImpl( DataType memberType, boolean memberIsNullable, FieldValueContainer container ) {
    this.memberType = memberType;
    this.memberIsNullable = memberIsNullable;
    this.container = container;
  }
  
  @Override
  public DataType memberType() {
    return memberType;
  }

  @Override
  public boolean memberIsNullable() {
    return memberIsNullable;
  }

  void bind( ArrayAccessor arrayAccessor ) {
    this.arrayAccessor = arrayAccessor;
  }

  @Override
  public int size() {
    if (arrayAccessor.isNull())
      return 0;
    return arrayAccessor.size();
  }
  
  @Override
  public FieldValue get(int i) {
    arrayAccessor.select( i );
    return container.get();
  }
}

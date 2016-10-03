package org.apache.drill.jig.types;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;

public class ArrayValueImpl implements ArrayValue {

  private final DataType memberType;
  private final FieldValueContainer container;
  private ArrayAccessor arrayAccessor;

  public ArrayValueImpl( DataType memberType, FieldValueContainer container ) {
    this.memberType = memberType;
    this.container = container;
  }
  
  @Override
  public DataType memberType() {
    return memberType;
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
//  public static abstract class BaseArrayValue extends ArrayValueImpl
//  {
//    protected final ArrayAccessor arrayAccessor;
//    
//    public BaseArrayValue( ArrayAccessor arrayAccessor, DataType memberType ) {
//      super( memberType );
//      this.arrayAccessor = arrayAccessor;
//    }
// 
//    @Override
//    public int size() {
//      if ( arrayAccessor.isNull() )
//        return 0;
//      return arrayAccessor.size();
//    }
//  }
//
//  public static class TypedArrayValue extends BaseArrayValue
//  {
//    protected final AbstractFieldValue memberValue;
// 
//    public TypedArrayValue( ArrayAccessor arrayAccessor, DataType memberType, AbstractFieldValue memberValue ) {
//      super( arrayAccessor, memberType );
//      this.memberValue = memberValue;
//    }
//    
//    @Override
//    public FieldValue get(int i) {
//      memberValue.bind( arrayAccessor.memberAccessor( i ) );
//      return memberValue;
//    }
//  }
//  
//  public static class VariantArrayValue extends BaseArrayValue
//  {
//    private final FieldValueCache values;
//
//    public VariantArrayValue( ArrayAccessor arrayAccessor, DataType memberType, FieldValueFactory factory ) {
//      super( arrayAccessor, memberType );
//      values = new FieldValueCache( factory );
//    }
//
//    @Override
//    public FieldValue get(int i) {
//      TypeAccessor typeAccessor = (TypeAccessor) arrayAccessor.memberAccessor( i );
//      AbstractFieldValue value = values.get( typeAccessor.getType() );
//      value.bind( typeAccessor );
//      return value;
//    }
//  }
}

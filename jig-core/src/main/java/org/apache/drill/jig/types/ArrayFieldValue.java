package org.apache.drill.jig.types;

import java.util.Collection;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.FieldAccessor.IndexedAccessor;
import org.apache.drill.jig.types.FieldAccessor.TypeAccessor;

public abstract class ArrayFieldValue extends AbstractStructuredValue {

  protected final DataType memberType;

  public ArrayFieldValue( DataType memberType ) {
    this.memberType = memberType;
  }
  
  @Override
  public DataType type() {
    return DataType.LIST;
  }

  @Override
  public DataType memberType() {
    return memberType;
  }

  @Override
  public Collection<String> keys() {
    throw mapError( );
  }

  @Override
  public FieldValue get(String key) {
    throw mapError( );
  }
  
  private ValueConversionError mapError( ) {
    return new ValueConversionError( "Cannot convert an array to a map" );
  }
  
  public static abstract class BaseArrayFieldValue extends ArrayFieldValue
  {
    protected ArrayAccessor arrayAccessor;
 
    public BaseArrayFieldValue( DataType memberType ) {
      super( memberType );
     }

    @Override
    public void bind(FieldAccessor accessor) {
      this.arrayAccessor = (ArrayAccessor) accessor;
    }

    @Override
    public boolean isNull() {
      return arrayAccessor.isNull();
    }

    @Override
    public Object getValue() {
       return arrayAccessor.getArray( );
    }

    @Override
    public int size() {
      if ( arrayAccessor.isNull() )
        return 0;
      return arrayAccessor.size();
    }
  }

  public static class TypedArrayFieldValue extends BaseArrayFieldValue
  {
    private final AbstractFieldValue memberValue;
 
    public TypedArrayFieldValue( DataType memberType, AbstractFieldValue memberValue ) {
      super( memberType );
      this.memberValue = memberValue;
    }
    
    @Override
    public FieldValue get(int i) {
      arrayAccessor.bind( i );
      memberValue.bind( arrayAccessor.memberAccessor() );
      return memberValue;
    }
  }
  
  public static class VariantArrayFieldValue extends BaseArrayFieldValue
  {
    private final FieldValueCache values;

    public VariantArrayFieldValue( DataType memberType, FieldValueFactory factory ) {
      super( memberType );
      values = new FieldValueCache( factory );
    }

    @Override
    public FieldValue get(int i) {
      arrayAccessor.bind( i );
      TypeAccessor typeAccessor = (TypeAccessor) arrayAccessor.memberAccessor();
      AbstractFieldValue value = values.get( typeAccessor.getType() );
      value.bind( typeAccessor );
      return value;
    }
  }

}

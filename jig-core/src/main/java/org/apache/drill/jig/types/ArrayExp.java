package org.apache.drill.jig.types;

import java.util.Collection;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.types.FieldAccessor.IndexedAccessor;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldAccessor.TypeAccessor;
import org.apache.drill.jig.types.FieldValueContainer.IndexableFieldValueContainer;

@Deprecated
public class ArrayExp {

//  public static abstract class AbstractArrayFieldValue extends AbstractStructuredValue {
//
//    protected final DataType memberType;
//
//    public AbstractArrayFieldValue( DataType memberType ) {
//      this.memberType = memberType;
//    }
//    
//    @Override
//    public DataType type() {
//      return DataType.LIST;
//    }
//
//    @Override
//    public DataType memberType() {
//      return memberType;
//    }
//
//    @Override
//    public Collection<String> keys() {
//      throw mapError( );
//    }
//
//    @Override
//    public FieldValue get(String key) {
//      throw mapError( );
//    }
//    
//    private ValueConversionError mapError( ) {
//      return new ValueConversionError( "Cannot convert an array to a map" );
//    }
//    
//  }
//  
//  public interface ArrayAccessor extends FieldAccessor
//  {
//    int size( );
//    Object getArray( );
//    IndexedAccessor memberAccessor( );
//  }
//  
////  public interface TypedIndexedObjectAccessor extends IndexedAccessor, TypeAccessor, ObjectAccessor
////  {
////    
////  }
////  
////  public interface JavaArrayAccessor extends ArrayAccessor, ObjectAccessor
////  {
////    Object getArray( );
////    IndexedAccessor memberAccessor( );
////  }
//  
//  public static class JavaArrayFieldValue extends AbstractArrayFieldValue
//  {
//    protected JavaArrayAccessor accessor;
//    private final IndexableFieldValueContainer fields;
//
//    public JavaArrayFieldValue( DataType memberType, FieldValueFactory factory ) {
//      super( memberType );
//      if ( memberType == DataType.VARIANT ) {
//        fields = new VariantIndexedFieldValueContainer( factory );
//      } else {
//        fields = new IndexedFieldValueContainer( factory );
//      }
//    }
//    @Override
//    public void bind(FieldAccessor accessor) {
//      this.accessor = (JavaArrayAccessor) accessor;
//      fields.bind( this.accessor.memberAccessor() );
//    }
//
//    @Override
//    public boolean isNull() {
//      // TODO Auto-generated method stub
//      return false;
//    }
//
//    @Override
//    public Object getValue() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public int size() {
//      // TODO Auto-generated method stub
//      return 0;
//    }
//
//    @Override
//    public FieldValue get(int i) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//  }
//  
//  public interface IndexedObjectAccessor extends IndexedAccessor, ObjectAccessor
//  {
//  }
//  
//  public static abstract class BaseArrayFieldValue extends AbstractArrayFieldValue
//  {
//    protected ArrayAccessor arrayAccessor;
// 
//    public BaseArrayFieldValue( DataType memberType ) {
//      super( memberType );
//     }
//
//    @Override
//    public void bind(FieldAccessor accessor) {
//      this.arrayAccessor = (ArrayAccessor) accessor;
//    }
//
//    @Override
//    public boolean isNull() {
//      return arrayAccessor.isNull();
//    }
//
//    @Override
//    public Object getValue() {
//       return arrayAccessor.getArray( );
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
//  public static class TypedArrayFieldValue extends BaseArrayFieldValue
//  {
//    private final AbstractFieldValue memberValue;
// 
//    public TypedArrayFieldValue( DataType memberType, AbstractFieldValue memberValue ) {
//      super( memberType );
//      this.memberValue = memberValue;
//    }
//    
//    @Override
//    public FieldValue get(int i) {
//      IndexedAccessor memberAccessor = arrayAccessor.memberAccessor();
//      memberAccessor.bind( i );
//      memberValue.bind( memberAccessor );
//      return memberValue;
//    }
//  }
//  
//  public static class VariantArrayFieldValue extends BaseArrayFieldValue
//  {
//    private final FieldValueCache values;
//
//    public VariantArrayFieldValue( DataType memberType, FieldValueFactory factory ) {
//      super( memberType );
//      values = new FieldValueCache( factory );
//    }
//
//    @Override
//    public FieldValue get(int i) {
//      IndexedAccessor memberAccessor = arrayAccessor.memberAccessor();
//      memberAccessor.bind( i );
//      TypeAccessor typeAccessor = (TypeAccessor) memberAccessor;
//      AbstractFieldValue value = values.get( typeAccessor.getType() );
//      value.bind( memberAccessor );
//      return value;
//    }
//  }
  
//  public abstract static class ArrayAccessor implements IndexedAccessor {
//
//    protected final ObjectAccessor arrayAccessor;
//    protected int memberIndex;
//
//    public ArrayAccessorOld( ObjectAccessor arrayAccessor ) {
//      this.arrayAccessor = arrayAccessor;
//    }
//
//    @Override
//    public void bind(int index) {
//      memberIndex = index;
//    }
    

}

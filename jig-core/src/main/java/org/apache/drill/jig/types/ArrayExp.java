package org.apache.drill.jig.types;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.types.BoxedAccessor.VariantBoxedAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayValueAccessor;
import org.apache.drill.jig.types.FieldAccessor.BooleanAccessor;
import org.apache.drill.jig.types.FieldAccessor.Float32Accessor;
import org.apache.drill.jig.types.FieldAccessor.Float64Accessor;
import org.apache.drill.jig.types.FieldAccessor.IndexedAccessor;
import org.apache.drill.jig.types.FieldAccessor.Int16Accessor;
import org.apache.drill.jig.types.FieldAccessor.Int32Accessor;
import org.apache.drill.jig.types.FieldAccessor.Int64Accessor;
import org.apache.drill.jig.types.FieldAccessor.Int8Accessor;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldAccessor.TypeAccessor;
import org.apache.drill.jig.types.JavaListAccessor.JavaListMemberAccessor;

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
    
//  public static class ArrayFieldValue extends AbstractStructuredValue {
//
//    private ArrayValueAccessor accessor;
//    
//    @Override
//    public void bind(FieldAccessor accessor) {
//      this.accessor = (ArrayValueAccessor) accessor;
//    }
//  
//    @Override
//    public DataType type() {
//      return DataType.LIST;
//    }
//  
//    @Override
//    public boolean isNull() {
//      return accessor.isNull();
//    }
//  
//    @Override
//    public MapValue getMap() {
//      throw new ValueConversionError( "Cannot convert a list to a map" );
//    }
//  
//    @Override
//    public ArrayValue getArray() {
//      return accessor.getArray( );
//    }
//  
//    @Override
//    public Object getValue() {
//      return accessor.getValue( );
//    }   
//  }
//  
//  public static abstract class AbstractArrayValue implements ArrayValue {
//
//    protected final DataType memberType;
//
//    public AbstractArrayValue( DataType memberType ) {
//      this.memberType = memberType;
//    }
//    
//    @Override
//    public DataType memberType() {
//      return memberType;
//    }
//  }
//
//  public static abstract class BaseArrayValue extends AbstractArrayValue
//  {
//    protected ArrayAccessor arrayAccessor;
// 
//    public BaseArrayValue( DataType memberType ) {
//      super( memberType );
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
//  public static abstract class TypedArrayValue extends BaseArrayValue
//  {
//    protected final AbstractFieldValue memberValue;
// 
//    public TypedArrayValue( DataType memberType, AbstractFieldValue memberValue ) {
//      super( memberType );
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
//    public VariantArrayValue( DataType memberType, FieldValueFactory factory ) {
//      super( memberType );
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
//  
//  public abstract class JavaArrayAccessor implements ArrayAccessor {
//
//    protected final ObjectAccessor arrayAccessor;
//    protected int memberIndex;
//
//    public JavaArrayAccessor( ObjectAccessor arrayAccessor ) {
//      this.arrayAccessor = arrayAccessor;
//    }
//    
//    @Override
//    public int size() {
//      Object array = getArray( );
//      if ( array == null )
//        return 0;
//      return Array.getLength( array );
//    }
//
//    @Override
//    public boolean isNull() {
//      return arrayAccessor.isNull();
//    }
//
//    @Override
//    public Object getValue( ) {
//      if ( arrayAccessor.isNull( ) )
//        return null;
//      return arrayAccessor.getObject( );
//    }
//    
//    protected Object prepareArray( ) {
//      Object array = getValue( );
//      if ( array == null )
//        throw new ValueConversionError( "Array is null ");
//      if ( memberIndex < 0  ||  memberIndex <= Array.getLength( array ) )
//        throw new ValueConversionError( "Index out of bounds: " + memberIndex + ", size = " + Array.getLength( array ) );
//      return array;
//    }
    
//    public static class JavaListAccessor extends ArrayAccessor implements ObjectAccessor {
  //
//      public JavaListAccessor( ObjectAccessor listAccessor ) {
//        super( listAccessor );
//      }   
//      
//      @Override
//      public boolean isNull() {
//        return getObject( ) == null;
//      }
//      
//      @Override
//      public Object getObject() {
//        List<? extends Object> list = getList( );
//        if ( list == null )
//          return null;
//        if ( memberIndex < 0  ||  memberIndex <= list.size( ) )
//          return null;
//        return list.get( memberIndex );
//      }
  //
//      @Override
//      public int size() {
//        List<? extends Object> list = getList( );
//        if ( list == null )
//          return 0;
//        return list.size( );
//      }
  //
//      @SuppressWarnings("unchecked")
//      private List<? extends Object> getList( ) {
//        if ( arrayAccessor.isNull( ) )
//          return null;
//        return (List<? extends Object>) arrayAccessor.getObject( );
//      }
//    }
    
//    public static abstract class JavaArrayAccessor extends ArrayAccessor {
  //
//      public JavaArrayAccessor(ObjectAccessor arrayAccessor) {
//        super(arrayAccessor);
//      }
  //
//    }
    
//    public static class ObjectArrayAccessor extends JavaArrayAccessor {
//      
//      private class ObjectArrayMemberAccessor implements ObjectAccessor {
//        
//        @Override
//        public boolean isNull() {
//          return getObject( ) == null;
//        }
//        
//        @Override
//        public Object getObject() {
//         return Array.get( prepareArray( ), memberIndex );
//        }
//      }
//      
//      private final ObjectArrayMemberAccessor memberAccessor = new ObjectArrayMemberAccessor( );
//
//      public ObjectArrayAccessor(ObjectAccessor arrayAccessor) {
//        super(arrayAccessor);
//      }
//
//      @Override
//      public FieldAccessor memberAccessor() {
//       return memberAccessor;
//      }
//    }
//    
//    public static class PrimitiveArrayAccessor extends JavaArrayAccessor {
//    
//      private class PrimitiveMemberAccessor implements FieldAccessor {
//
//        @Override
//        public boolean isNull() {
//          return false;
//        }
//      }
//      
//      private class BooleanMemberAccessor extends PrimitiveMemberAccessor implements BooleanAccessor {
//
//        @Override
//        public boolean getBoolean() {    
//          return ((boolean[]) prepareArray( ))[memberIndex];
//        }   
//      }
//      
//      private class Int8MemberAccessor extends PrimitiveMemberAccessor implements Int8Accessor {
//
//        @Override
//        public byte getByte() {    
//          return ((byte[]) prepareArray( ))[memberIndex];
//        }   
//      }
//      
//      private class Int16MemberAccessor extends PrimitiveMemberAccessor implements Int16Accessor {
//
//        @Override
//        public short getShort() {    
//          return ((short[]) prepareArray( ))[memberIndex];
//        }   
//      }
//      
//      private class Int32MemberAccessor extends PrimitiveMemberAccessor implements Int32Accessor {
//
//        @Override
//        public int getInt() {    
//          return ((int[]) prepareArray( ))[memberIndex];
//        }   
//      }
//      
//      private class Int64MemberAccessor extends PrimitiveMemberAccessor implements Int64Accessor {
//
//        @Override
//        public long getLong() {    
//          return ((long[]) prepareArray( ))[memberIndex];
//        }   
//      }
//      
//      private class Float32MemberAccessor extends PrimitiveMemberAccessor implements Float32Accessor {
//
//       @Override
//        public float getFloat() {    
//          return ((float[]) prepareArray( ))[memberIndex];
//        }   
//      }
//      
//      private class Float64MemberAccessor extends PrimitiveMemberAccessor implements Float64Accessor {
//
//        @Override
//        public double getDouble() {    
//          return ((double[]) prepareArray( ))[memberIndex];
//        }   
//      }
//
//      private final FieldAccessor memberAccessor;
//      
//      public PrimitiveArrayAccessor( ObjectAccessor arrayAccessor, DataType memberType ) {
//        super(arrayAccessor);
//        switch ( memberType ) {
//        case BOOLEAN:
//          memberAccessor = new BooleanMemberAccessor( );
//          break;
//        case FLOAT32:
//          memberAccessor = new Float32MemberAccessor( );
//          break;
//        case FLOAT64:
//          memberAccessor = new Float64MemberAccessor( );
//          break;
//        case INT16:
//          memberAccessor = new Int16MemberAccessor( );
//          break;
//        case INT32:
//          memberAccessor = new Int32MemberAccessor( );
//          break;
//        case INT64:
//          memberAccessor = new Int64MemberAccessor( );
//          break;
//        case INT8:
//          memberAccessor = new Int8MemberAccessor( );
//          break;
//        default:
//          throw new IllegalStateException( "Not a scalar type: " + memberType );
//        }
//      }
//
//      @Override
//      public FieldAccessor memberAccessor() {
//        return memberAccessor;
//      }
//    }
//  }
//  
//  public static class JavaListAccessor implements ArrayAccessor {
//    
//    public class JavaListMemberAccessor implements IndexedAccessor, ObjectAccessor {
//
//      private int index;
//      
//      @Override
//      public void bind( int index ) {
//        this.index = index;
//      }
//      
//      @Override
//      public boolean isNull() {
//        return getObject( ) == null;
//      }
//
//      @Override
//      public Object getObject() {
//        return getList( ).get( index );
//      }    
//    }
//
//    private final ObjectAccessor listAccessor;
//    private final JavaListMemberAccessor memberAccessor = new JavaListMemberAccessor( );
//    private final BoxedAccessor memberValueAccessor;
//
//    public JavaListAccessor( ObjectAccessor listAccessor ) {
//      this.listAccessor = listAccessor;
//      memberValueAccessor = new BoxedAccessor( memberAccessor );
//    }
//
//    public JavaListAccessor( ObjectAccessor listAccessor, FieldValueFactory factory ) {
//      this.listAccessor = listAccessor;
//      memberValueAccessor = new VariantBoxedAccessor( memberAccessor, factory );
//    }
//
//    @Override
//    public int size() {
//      if ( listAccessor.isNull() )
//        return 0;
//      return getList( ).size( );
//    }
//
//    @Override
//    public Object getValue() {
//      return listAccessor.getObject();
//    }
//
//    @Override
//    public boolean isNull() {
//      return listAccessor.isNull();
//    }
//    
//    @SuppressWarnings("unchecked")
//    private List<? extends Object> getList( ) {
//      if ( listAccessor.isNull( ) )
//        return null;
//      return (List<? extends Object>) listAccessor.getObject( );
//    }
//
//    @Override
//    public FieldAccessor memberAccessor( int index ) {
//      memberAccessor.bind( index );
//      return memberValueAccessor;
//    }
//  }
    
}

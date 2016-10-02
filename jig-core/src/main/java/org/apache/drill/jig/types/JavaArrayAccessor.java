package org.apache.drill.jig.types;

import java.lang.reflect.Array;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;

public abstract class JavaArrayAccessor implements ArrayAccessor {

  protected final ObjectAccessor arrayAccessor;
  protected int memberIndex;

  public JavaArrayAccessor( ObjectAccessor arrayAccessor ) {
    this.arrayAccessor = arrayAccessor;
  }

  @Override
  public void bind(int index) {
    memberIndex = index;
  }
  
  @Override
  public int size() {
    Object array = getArray( );
    if ( array == null )
      return 0;
    return Array.getLength( array );
  }

  @Override
  public boolean isNull() {
    return arrayAccessor.isNull();
  }

  @Override
  public Object getArray( ) {
    if ( arrayAccessor.isNull( ) )
      return null;
    return arrayAccessor.getObject( );
  }
  
  protected Object prepareArray( ) {
    Object array = getArray( );
    if ( array == null )
      throw new ValueConversionError( "Array is null ");
    if ( memberIndex < 0  ||  memberIndex <= Array.getLength( array ) )
      throw new ValueConversionError( "Index out of bounds: " + memberIndex + ", size = " + Array.getLength( array ) );
    return array;
  }
  
//  public static class JavaListAccessor extends ArrayAccessor implements ObjectAccessor {
//
//    public JavaListAccessor( ObjectAccessor listAccessor ) {
//      super( listAccessor );
//    }   
//    
//    @Override
//    public boolean isNull() {
//      return getObject( ) == null;
//    }
//    
//    @Override
//    public Object getObject() {
//      List<? extends Object> list = getList( );
//      if ( list == null )
//        return null;
//      if ( memberIndex < 0  ||  memberIndex <= list.size( ) )
//        return null;
//      return list.get( memberIndex );
//    }
//
//    @Override
//    public int size() {
//      List<? extends Object> list = getList( );
//      if ( list == null )
//        return 0;
//      return list.size( );
//    }
//
//    @SuppressWarnings("unchecked")
//    private List<? extends Object> getList( ) {
//      if ( arrayAccessor.isNull( ) )
//        return null;
//      return (List<? extends Object>) arrayAccessor.getObject( );
//    }
//  }
  
//  public static abstract class JavaArrayAccessor extends ArrayAccessor {
//
//    public JavaArrayAccessor(ObjectAccessor arrayAccessor) {
//      super(arrayAccessor);
//    }
//
//  }
  
  public static class ObjectArrayAccessor extends JavaArrayAccessor {
    
    private class ObjectArrayMemberAccessor implements ObjectAccessor {
      
      @Override
      public boolean isNull() {
        return getObject( ) == null;
      }
      
      @Override
      public Object getObject() {
       return Array.get( prepareArray( ), memberIndex );
      }
    }
    
    private final ObjectArrayMemberAccessor memberAccessor = new ObjectArrayMemberAccessor( );

    public ObjectArrayAccessor(ObjectAccessor arrayAccessor) {
      super(arrayAccessor);
    }

    @Override
    public FieldAccessor memberAccessor() {
     return memberAccessor;
    }
  }
  
  public static class PrimitiveArrayAccessor extends JavaArrayAccessor {
  
    private class PrimitiveMemberAccessor implements FieldAccessor {

      @Override
      public boolean isNull() {
        return false;
      }
    }
    
    private class BooleanMemberAccessor extends PrimitiveMemberAccessor implements BooleanAccessor {

      @Override
      public boolean getBoolean() {    
        return ((boolean[]) prepareArray( ))[memberIndex];
      }   
    }
    
    private class Int8MemberAccessor extends PrimitiveMemberAccessor implements Int8Accessor {

      @Override
      public byte getByte() {    
        return ((byte[]) prepareArray( ))[memberIndex];
      }   
    }
    
    private class Int16MemberAccessor extends PrimitiveMemberAccessor implements Int16Accessor {

      @Override
      public short getShort() {    
        return ((short[]) prepareArray( ))[memberIndex];
      }   
    }
    
    private class Int32MemberAccessor extends PrimitiveMemberAccessor implements Int32Accessor {

      @Override
      public int getInt() {    
        return ((int[]) prepareArray( ))[memberIndex];
      }   
    }
    
    private class Int64MemberAccessor extends PrimitiveMemberAccessor implements Int64Accessor {

      @Override
      public long getLong() {    
        return ((long[]) prepareArray( ))[memberIndex];
      }   
    }
    
    private class Float32MemberAccessor extends PrimitiveMemberAccessor implements Float32Accessor {

     @Override
      public float getFloat() {    
        return ((float[]) prepareArray( ))[memberIndex];
      }   
    }
    
    private class Float64MemberAccessor extends PrimitiveMemberAccessor implements Float64Accessor {

      @Override
      public double getDouble() {    
        return ((double[]) prepareArray( ))[memberIndex];
      }   
    }

    private final FieldAccessor memberAccessor;
    
    public PrimitiveArrayAccessor( ObjectAccessor arrayAccessor, DataType memberType ) {
      super(arrayAccessor);
      switch ( memberType ) {
      case BOOLEAN:
        memberAccessor = new BooleanMemberAccessor( );
        break;
      case FLOAT32:
        memberAccessor = new Float32MemberAccessor( );
        break;
      case FLOAT64:
        memberAccessor = new Float64MemberAccessor( );
        break;
      case INT16:
        memberAccessor = new Int16MemberAccessor( );
        break;
      case INT32:
        memberAccessor = new Int32MemberAccessor( );
        break;
      case INT64:
        memberAccessor = new Int64MemberAccessor( );
        break;
      case INT8:
        memberAccessor = new Int8MemberAccessor( );
        break;
      default:
        throw new IllegalStateException( "Not a scalar type: " + memberType );
      }
    }

    @Override
    public FieldAccessor memberAccessor() {
      return memberAccessor;
    }
  }

//    public PrimitiveArrayAccessor(ObjectAccessor arrayAccessor) {
//      super(arrayAccessor);
//    }
//
//    @Override
//    public boolean isNull() {
//      return false;
//    }
//    
//    @Override
//    protected Object getArray( ) {
//      Object array = getArray( );
//      if ( array == null )
//        throw new ValueConversionError( "Array is null ");
//      if ( memberIndex < 0  ||  memberIndex <= Array.getLength( array ) )
//        throw new ValueConversionError( "Index out of bounds: " + memberIndex + ", size = " + Array.getLength( array ) );
//      return array;
//    }
//  }
    
}

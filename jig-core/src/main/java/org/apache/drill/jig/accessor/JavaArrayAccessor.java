package org.apache.drill.jig.accessor;

import java.lang.reflect.Array;

import org.apache.drill.jig.accessor.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ValueObjectAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.util.JigUtilities;

/**
 * Array accessor for arrays backed by a Java array. The Java array can be a primitive
 * array (of int, say) or of Java objects. If of Java objects, the array can include
 * objects all of a single type (Integer, say), or of varying types.
 * <p>
 * This accessor needs access to the Java array object, which it obtains from a
 * Java object accessor. The input accessor can fetch the object, build it or do
 * whatever else is needed to get the array.
 * <p>
 * Use these classes when building a {@link DataDef.ArrayDef}, which will build the
 * various array value and array member accessors, and the {@link FieldValue}.
 */

public abstract class JavaArrayAccessor implements ArrayAccessor, ValueObjectAccessor {

  protected abstract class AbstractMemberAccessor implements IndexedAccessor
  {
    protected int index;

    @Override
    public void bind(int index) {
      this.index = index;
    }

    protected Object getArray( ) {
      return prepareArray( index );
    }

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      buf.append( "]" );
    }
  }

  protected final ObjectAccessor arrayAccessor;
  protected IndexedAccessor elementAccessor;

  public JavaArrayAccessor( ObjectAccessor arrayAccessor ) {
    this.arrayAccessor = arrayAccessor;
  }

  @Override
  public int size() {
    Object array = getValue( );
    if ( array == null )
      return 0;
    return Array.getLength( array );
  }

  @Override
  public boolean isNull() {
    return arrayAccessor.isNull();
  }

  @Override
  public Object getValue( ) {
    if ( arrayAccessor.isNull( ) )
      return null;
    return arrayAccessor.getObject( );
  }

  @Override
  public FieldAccessor elementAccessor( ) {
    return elementAccessor;
  }

  @Override
  public void select( int i ) {
    elementAccessor.bind( i );
  }

  protected Object prepareArray( int memberIndex ) {
    Object array = getValue( );
    if ( array == null )
      throw new ValueConversionError( "Array is null ");
    if ( memberIndex < 0  ||  Array.getLength( array ) <= memberIndex )
      throw new ValueConversionError( "Index out of bounds: " + memberIndex + ", size = " + Array.getLength( array ) );
    return array;
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "array accessor = " );
    arrayAccessor.visualize( buf, indent + 2 );
    buf.append( "\n" );
    buf.append( "element accessor = " );
    elementAccessor.visualize( buf, indent + 2 );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent );
    buf.append( "]" );
  }

  public static class ObjectArrayAccessor extends JavaArrayAccessor {

    private class ObjectArrayMemberAccessor extends AbstractMemberAccessor implements ObjectAccessor {

      @Override
      public boolean isNull() {
        return getObject( ) == null;
      }

      @Override
      public Object getObject() {
       return Array.get( getArray( ), index );
      }
    }

   public ObjectArrayAccessor(ObjectAccessor arrayAccessor) {
      super( arrayAccessor );
      elementAccessor = new ObjectArrayMemberAccessor( );
    }
  }

  public static class PrimitiveArrayAccessor extends JavaArrayAccessor {

    private class PrimitiveMemberAccessor extends AbstractMemberAccessor implements FieldAccessor {

      @Override
      public boolean isNull() {
        return false;
      }
    }

    private class BooleanMemberAccessor extends PrimitiveMemberAccessor implements BooleanAccessor {

      @Override
      public boolean getBoolean() {
        return ((boolean[]) getArray( ))[index];
      }
    }

    private class Int8MemberAccessor extends PrimitiveMemberAccessor implements Int8Accessor {

      @Override
      public byte getByte() {
        return ((byte[]) getArray( ))[index];
      }
    }

    private class Int16MemberAccessor extends PrimitiveMemberAccessor implements Int16Accessor {

      @Override
      public short getShort() {
        return ((short[]) getArray( ))[index];
      }
    }

    private class Int32MemberAccessor extends PrimitiveMemberAccessor implements Int32Accessor {

      @Override
      public int getInt() {
        return ((int[]) getArray( ))[index];
      }
    }

    private class Int64MemberAccessor extends PrimitiveMemberAccessor implements Int64Accessor {

      @Override
      public long getLong() {
        return ((long[]) getArray( ))[index];
      }
    }

    private class Float32MemberAccessor extends PrimitiveMemberAccessor implements Float32Accessor {

     @Override
      public float getFloat() {
        return ((float[]) getArray( ))[index];
      }
    }

    private class Float64MemberAccessor extends PrimitiveMemberAccessor implements Float64Accessor {

      @Override
      public double getDouble() {
        return ((double[]) getArray( ))[index];
      }
    }

    public PrimitiveArrayAccessor( ObjectAccessor arrayAccessor, DataType memberType ) {
      super(arrayAccessor);
      switch ( memberType ) {
      case BOOLEAN:
        elementAccessor = new BooleanMemberAccessor( );
        break;
      case FLOAT32:
        elementAccessor = new Float32MemberAccessor( );
        break;
      case FLOAT64:
        elementAccessor = new Float64MemberAccessor( );
        break;
      case INT16:
        elementAccessor = new Int16MemberAccessor( );
        break;
      case INT32:
        elementAccessor = new Int32MemberAccessor( );
        break;
      case INT64:
        elementAccessor = new Int64MemberAccessor( );
        break;
      case INT8:
        elementAccessor = new Int8MemberAccessor( );
        break;
      default:
        throw new IllegalStateException( "Not a scalar type: " + memberType );
      }
    }
  }
}

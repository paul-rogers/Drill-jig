package org.apache.drill.jig.serde.deserializer;

import java.math.BigDecimal;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.serde.deserializer.BufferScalarAccessor.BufferMemberAccessor;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.FieldValueCache;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.FieldAccessor.*;

public abstract class BufferArrayAccessor implements ObjectAccessor, Resetable {

  private Object cached;
  protected int index;
  protected TupleSetDeserializer deserializer;
  
  public void bind( TupleSetDeserializer deserializer, int index ) {
    this.deserializer = deserializer;
    this.index = index;
  }
  
  @Override
  public void reset() {
    cached = null;
  }

  @Override
  public boolean isNull() {
    return deserializer.isNull( index );
  }
  
  protected void seek( ) {
    deserializer.seek( index );
  }
  
  /**
   * If the array is a top-level field, it starts with a field
   * length, whch we skip here.
   */
  
  protected void skipHeader( ) {
    deserializer.seek( index );
    deserializer.reader( ).readInt( ); // Skip field length
  }
  
  /**
   * Whether the array is top-level or embedded in another array,
   * each array has an element count which we read here.
   * @return
   */
  
  protected int readSize( ) {
    return deserializer.reader( ).readIntEncoded();
  }
  
  @Override
  public Object getObject() {
    if ( cached == null ) {
      skipHeader( );
      cached = buildArray( );
    }
    return cached;
  }
  
  protected abstract Object buildArray( );

  public static class BooleanArrayAccessor extends BufferArrayAccessor {

    @Override
    protected Object buildArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      boolean array[] = new boolean[ size ];
      int posn = 0;
      int byteCount = (size + 7) / 8;
      for ( int i = 0;  i < byteCount;  i++ ) {
        int b = reader.readByte();
        int mask = 0x80;
        for ( int j = 0;  j < 8  &&  posn < size;  j++ ) {
          array[posn] = (b & mask) != 0;
          mask >>= 1;
        }
      }
      return array;
    }    
  }

  public static class Int8ArrayAccessor extends BufferArrayAccessor {

    @Override
    protected Object buildArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      byte array[] = new byte[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readByte();
      }
      return array;
    }  
  }

  public static class Int16ArrayAccessor extends BufferArrayAccessor {

    @Override
    protected Object buildArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      short array[] = new short[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readShort();
      }
      return array;
    }  
  }
  
  public static class Int32ArrayAccessor extends BufferArrayAccessor {

    @Override
    protected Object buildArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      int array[] = new int[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readIntEncoded();
      }
      return array;
    }  
  }
  
  public static class Int64ArrayAccessor extends BufferArrayAccessor {

    @Override
    protected Object buildArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      long array[] = new long[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readLongEncoded();
      }
      return array;
    }  
  }
  
  public static class Float32ArrayAccessor extends BufferArrayAccessor {

    @Override
    protected Object buildArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      float array[] = new float[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readFloat();
      }
      return array;
    }  
  }
  
  public static class Float64ArrayAccessor extends BufferArrayAccessor {

    @Override
    protected Object buildArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      double array[] = new double[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readDouble();
      }
      return array;
    }  
  }
  
  public abstract static class BufferObjectArrayAccessor extends BufferArrayAccessor {

    private boolean nullable;
    
    public BufferObjectArrayAccessor( boolean nullable ) {
      this.nullable = nullable;
    }    

    @Override
    protected Object buildArray() {
      if ( nullable )
        return buildNullableArray( );
      else
        return buildNonNullableArray( );
    }
    
    protected Object buildNonNullableArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      Object array[] = new Object[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = readObject( reader );
      }
      return array;
    }  

    protected Object buildNullableArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      boolean isNull[] = readNullFlags( reader, size );
      Object array[] = new Object[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        if ( !isNull[i] )
          array[i] = readObject( reader );
      }
      return array;
    }
    
    protected abstract Object readObject( TupleReader reader );
    
    protected boolean[] readNullFlags( TupleReader reader, int n ) {
      boolean isNull[] = new boolean[n];
      int byteCount = (n+7)/8;
      int posn = 0;
      for ( int i = 0;  i < byteCount;  i++ ) {
        int flags = reader.readByte();
        for ( int j = 0;  j < 8  &&  posn < n;  j++ ) {
          isNull[posn++] = (flags & 0x80) != 0;
          flags <<= 1;
        }
      }
      return isNull;     
    }
  }
  
  public static class DecimalArrayAccessor extends BufferObjectArrayAccessor {

    public DecimalArrayAccessor(boolean nullable) {
      super(nullable);
    }

    @Override
    protected Object readObject(TupleReader reader) {
      return reader.readDecimal();
    }  
  }
  
  public static class StringArrayAccessor extends BufferObjectArrayAccessor {

    public StringArrayAccessor(boolean nullable) {
      super(nullable);
    }

    @Override
    protected Object readObject(TupleReader reader) {
      return reader.readString();
    }  
  }
  
  public static class VariantArrayAccessor extends BufferArrayAccessor {

    private final FieldValueCache valueCache;
    private BufferMemberAccessor accessor;
    
    public VariantArrayAccessor( FieldValueFactory factory ) {
      valueCache = new FieldValueCache( factory );
      accessor = new BufferMemberAccessor( );
    }

    public void bind( TupleSetDeserializer deserializer, int index ) {
      super.bind( deserializer, index );
      accessor.bind( deserializer.reader );
    }

    @Override
    protected Object buildArray() {
      int size = readSize( );
      TupleReader reader = deserializer.reader( );
      Object array[] = new Object[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        DataType type = DataType.typeForCode( reader.readByte() );
        AbstractFieldValue value = valueCache.get( type );
        value.bind( accessor );
        array[i] = value.getValue( );
      }
      return array;
    }  
  }
  
  public static class ArrayOfArrayAccessor extends BufferArrayAccessor {

    private BufferArrayAccessor innerAccessor;
    
    public ArrayOfArrayAccessor( BufferArrayAccessor innerAccessor ) {
      this.innerAccessor = innerAccessor;
    }

    public void bind( TupleSetDeserializer deserializer, int index ) {
      super.bind( deserializer, index );
      innerAccessor.bind( deserializer, 0 );
    }

    @Override
    protected Object buildArray() {
      int size = readSize( );
      Object array[] = new Object[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = innerAccessor.buildArray();
      }
      return array;
    }  
  }
}

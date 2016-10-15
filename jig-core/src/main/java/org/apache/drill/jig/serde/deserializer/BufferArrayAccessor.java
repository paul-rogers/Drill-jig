package org.apache.drill.jig.serde.deserializer;

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
  
  protected int readSize( ) {
    deserializer.seek( index );
    TupleReader reader = deserializer.reader( );
    reader.readInt( ); // Skip field length
    return reader.readIntEncoded();
  }
  
  @Override
  public Object getObject() {
    if ( cached == null )
      cached = buildArray( );
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
}

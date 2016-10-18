package org.apache.drill.jig.serde.deserializer;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.serde.deserializer.BufferScalarAccessor.BufferMemberAccessor;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.FieldValueCache;
import org.apache.drill.jig.types.FieldValueFactory;

public abstract class BufferStructureAccessor implements ObjectAccessor {

  protected int index;
  protected TupleSetDeserializer deserializer;
  
  public void bind( TupleSetDeserializer deserializer, int index ) {
    this.deserializer = deserializer;
    this.index = index;
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
   * length, which we skip here.
   */
  
  protected void skipHeader( ) {
    seek( );
    deserializer.reader( ).readInt( ); // Skip field length
  }
  
  @Override
  public Object getObject() {
    if ( isNull( ) )
      return null;
  
    skipHeader( );
    return buildStructure( );
  }
  
  protected abstract Object buildStructure( );

  public static class BooleanArrayAccessor extends BufferStructureAccessor {

    @Override
    protected Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
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

  public static class Int8ArrayAccessor extends BufferStructureAccessor {

    @Override
    protected Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
      byte array[] = new byte[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readByte();
      }
      return array;
    }  
  }

  public static class Int16ArrayAccessor extends BufferStructureAccessor {

    @Override
    protected Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
      short array[] = new short[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readShort();
      }
      return array;
    }  
  }
  
  public static class Int32ArrayAccessor extends BufferStructureAccessor {

    @Override
    protected Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
      int array[] = new int[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readIntEncoded();
      }
      return array;
    }  
  }
  
  public static class Int64ArrayAccessor extends BufferStructureAccessor {

    @Override
    protected Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
      long array[] = new long[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readLongEncoded();
      }
      return array;
    }  
  }
  
  public static class Float32ArrayAccessor extends BufferStructureAccessor {

    @Override
    protected Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
      float array[] = new float[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readFloat();
      }
      return array;
    }  
  }
  
  public static class Float64ArrayAccessor extends BufferStructureAccessor {

    @Override
    protected Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
      double array[] = new double[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = reader.readDouble();
      }
      return array;
    }  
  }
  
  /**
   * Accesses a serialized array by building a Java object array to hold
   * the deserialized objects. Handles both nullable and non-nullable array
   * members. If the members are nullable, then writes an array of flags
   * to indicate which members are null, and only reads the non-null
   * members.
   */
  
  public abstract static class BufferObjectArrayAccessor extends BufferStructureAccessor {

    private boolean nullable;
    
    public BufferObjectArrayAccessor( boolean nullable ) {
      this.nullable = nullable;
    }    

    @Override
    protected Object buildStructure() {
      if ( nullable )
        return buildNullableArray( );
      else
        return buildNonNullableArray( );
    }
    
    protected Object buildNonNullableArray() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
      Object array[] = new Object[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        array[i] = readObject( reader );
      }
      return array;
    }  

    protected Object buildNullableArray() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
      boolean isNull[] = readNullFlags( reader, size );
      Object array[] = new Object[ size ];
      for ( int i = 0;  i < size;  i++ ) {
        if ( !isNull[i] )
          array[i] = readObject( reader );
      }
      return array;
    }
    
    protected abstract Object readObject( TupleReader reader );
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
  
  /**
   * Reads an array of Variants into a Java Object array.
   */
  
  public static class VariantArrayAccessor extends BufferStructureAccessor {

    private final FieldValueCache valueCache;
    private BufferMemberAccessor accessor;
    
    public VariantArrayAccessor( FieldValueFactory factory ) {
      valueCache = new FieldValueCache( factory );
      accessor = new BufferMemberAccessor( );
    }

    @Override
    public void bind( TupleSetDeserializer deserializer, int index ) {
      super.bind( deserializer, index );
      accessor.bind( deserializer.reader );
    }

    @Override
    protected Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int size = reader.readIntEncoded();
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
  
  /**
   * Reads an array of structured types into a Java Object array.
   */
  
  public static class ArrayOfStructureAccessor extends BufferObjectArrayAccessor {

    private final BufferStructureAccessor innerAccessor;
    
    /**
     * Constructor.
     * @param innerAccessor accesssor for the structure that appears as
     * each element of the array.
     */
    
    public ArrayOfStructureAccessor( BufferStructureAccessor innerAccessor, boolean nullable ) {
      super( nullable );
      this.innerAccessor = innerAccessor;
    }

    @Override
    public void bind( TupleSetDeserializer deserializer, int index ) {
      super.bind( deserializer, index );
      innerAccessor.bind( deserializer, 0 );
    }
    
    @Override
    protected Object readObject(TupleReader reader) {
      return innerAccessor.buildStructure();
    }
  }
  
  /**
   * Reads a serialized map into a Java Map object.
   */
  
  public static class BufferMapAccessor extends BufferStructureAccessor {

    private final FieldValueCache valueCache;
    private BufferMemberAccessor valueAccessor;
    
    public BufferMapAccessor( FieldValueFactory factory ) {
      valueCache = new FieldValueCache( factory );
      valueAccessor = new BufferMemberAccessor( );
    }
    
    @Override
    public void bind( TupleSetDeserializer deserializer, int index ) {
      super.bind( deserializer, index);
      valueAccessor.bind( deserializer, 0 );
    }
    
    @Override
    public Object buildStructure() {
      TupleReader reader = deserializer.reader( );
      int count = reader.readIntEncoded();
      Map<String,Object> map = new HashMap<>( );
      for ( int i = 0;  i < count;  i++ ) {
        String key = reader.readString();
        DataType type = DataType.typeForCode( reader.readByte() );
        AbstractFieldValue value = valueCache.get( type );
        value.bind( valueAccessor );
        map.put( key, value.getValue( ) );
      }
      return map;
    }
  }
  
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

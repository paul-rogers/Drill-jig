package org.apache.drill.jig.serde.deserializer;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.ScalarValue;
import org.apache.drill.jig.types.FieldAccessor;
import org.apache.drill.jig.types.FieldAccessor.BooleanAccessor;
import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.serde.deserializer.TupleSetDeserializer.DeserializedTupleAccessor;

public abstract class BufferFieldAccessor implements FieldAccessor
{
//  protected int fieldIndex;
//  
//  public void bind( int fieldIndex ) {
//    this.fieldIndex = fieldIndex;
//  }
//  
//  @Override
//  public boolean isNull() {
//    // TODO Auto-generated method stub
//    return false;
//  }

  public static class BooleanBufferAccessor extends BufferFieldAccessor implements BooleanAccessor
  {
    @Override
    public boolean getBoolean() {
      seek( );
      return reader.readBoolean();
    }   
  }
//  private abstract static class BufferScalarAccessor extends BufferFieldAccessor implements ScalarValue
//  {
////    @Override
////    public ScalarValue asScalar() {
////      verifyNotNull( );
////      return this;
////    }
//
//    @Override
//    public boolean getBoolean() {
//      throw notSupportedError( DataType.BOOLEAN.displayName() );
//    }   
//
//    @Override
//    public byte getByte() {
//      throw notSupportedError( DataType.INT8.displayName() );
//    }
//
//    @Override
//    public int getInt() {
//      throw notSupportedError( DataType.INT32.displayName( ) );
//    }
//
//    @Override
//    public short getShort() {
//      throw notSupportedError( DataType.INT16.displayName() );
//    }
//
//    @Override
//    public long getLong() {
//      throw notSupportedError( DataType.INT64.displayName() );
//    }
//
//    @Override
//    public float getFloat() {
//      throw notSupportedError( DataType.STRING.displayName() );
//    }
//
//    @Override
//    public double getDouble() {
//      throw notSupportedError( DataType.FLOAT64.displayName() );
//    }
//
//    @Override
//    public BigDecimal getDecimal() {
//      throw notSupportedError( DataType.DECIMAL.displayName() );
//    }
//
//    @Override
//    public String getString() {
//      throw notSupportedError( DataType.STRING.displayName() );
//    }
//
//    @Override
//    public byte[] getBlob() {
//      throw notSupportedError( DataType.BLOB.displayName() );
//    }
//
//    @Override
//    public LocalDate getDate() {
//      throw notSupportedError( DataType.DATE.displayName() );
//    }
//
//    @Override
//    public LocalDateTime getDateTime() {
//      throw notSupportedError( DataType.LOCAL_DATE_TIME.displayName() );
//    }
//
//    @Override
//    public Period getUTCTime() {
//      throw notSupportedError( DataType.UTC_DATE_TIME.displayName() );
//    }
//
//    @Override
//    public Object getValue() {
//      throw notSupportedError( "Object" );
//    }
//    
//  }
//  
//  private static class BufferBooleanAccessor extends BufferScalarAccessor
//  {
//    @Override
//    public boolean getBoolean() {
//      seek( );
//      return reader.readBoolean();
//    }
//    
//    @Override
//    public Object getValue() {
//      return getBoolean( );
//    }
//  }
//  

  public static class BufferInt8Accessor extends BufferFieldAccessor implements Int8Accessor
  {
//  private static class BufferInt8Accessor extends BufferScalarAccessor
//  {
    @Override
    public byte getByte() {
      seek( );
      return reader.readByte();
    }
//    
//    @Override
//    public Object getValue() {
//      return getByte( );
//    }
  }
//  
  private static class BufferInt16Accessor extends BufferFieldAccessor implements Int16Accessor
  {
    @Override
    public short getShort() {
      seek( );
      return reader.readShort();
    }
//    
//    @Override
//    public Object getValue() {
//      return getShort( );
//    }
  }
//  
  private static class BufferInt32Accessor extends BufferFieldAccessor implements Int32Accessor
  {
    @Override
    public int getInt() {
      seek( );
      return reader.readIntEncoded();
    }
//    
//    @Override
//    public Object getValue() {
//      return getInt( );
//    }
  }
  
  private static class BufferInt64Accessor extends BufferFieldAccessor implements Int64Accessor
  {
    @Override
    public long getLong() {
      seek( );
      return reader.readLongEncoded();
    }
//    
//    @Override
//    public int getInt() {
//      return (int) getLong( );
//    }
//
//    @Override
//    public Object getValue() {
//      return getLong( );
//    }
  }
  
  private static class BufferFloat32Accessor extends BufferFieldAccessor implements Float32Accessor
  {
    @Override
    public float getFloat() {
      seek( );
      return reader.readFloat();
    }
//    
//    @Override
//    public Object getValue() {
//      return getFloat( );
//    }
  }
  
  private static class BufferFloat64Accessor extends BufferFieldAccessor implements Float64Accessor
  {
    @Override
    public double getDouble() {
      seek( );
      return reader.readDouble();
    }
//    
//    @Override
//    public Object getValue() {
//      return getDouble( );
//    }
  }
  
  private static class BufferDecimalAccessor extends BufferFieldAccessor implements DecimalAccessor
  {
    @Override
    public BigDecimal getDecimal() {
      seek( );
      return reader.readDecimal();
    }
    
//    @Override
//    public Object getValue() {
//      return getDecimal( );
//    }
  }
  
  private static class BufferStringAccessor extends BufferFieldAccessor implements StringAccessor
  {
    @Override
    public String getString() {
      seek( );
      return reader.readString();
    }
//    
//    @Override
//    public Object getValue() {
//      return getString( );
//    }
  }
//  ,
////  private static class BufferAnyAccessor extends BufferFieldAccessor implements AnyAccessor
////  {
////    @Override
////    public ScalarValue asScalar() {
////      verifyNotNull( );
////      return this;
////    }
////
////    @Override
////    public AnyAccessor asAny() {
////      verifyNotNull( );
////      return this;
////    }
////
////    @Override
////    public DataType getDataType() {
////      seek( );
////      return DataType.typeForCode( reader.readByte() );
////    }
////    
////    @Override
////    public boolean getBoolean() {
////      if ( getDataType( ) == DataType.BOOLEAN )
////        return reader.readBoolean();
////      throw notSupportedError( DataType.BOOLEAN.displayName() );
////    }
////
////    @Override
////    public Object getValue() {
////      switch( getDataType( ) ) {
////      case BOOLEAN:
////        return reader.readBoolean();
////      case INT8:
////        return reader.readByte();
////      case INT16:
////        return reader.readShort();
////      case INT32:
////        return reader.readIntEncoded();
////      case INT64:
////        return reader.readLongEncoded();
////      case FLOAT32:
////        return reader.readFloat( );
////      case FLOAT64:
////        return reader.readDouble( );
////      case DECIMAL:
////        return reader.readDecimal();
////      case STRING:
////        return reader.readString();
////      default:
////        return null;
////      }
////    }
////
////    @Override
////    public byte getByte() {
////      if ( getDataType( ) == DataType.INT8 )
////        return reader.readByte( );
////      throw notSupportedError( DataType.INT8.displayName() );
////    }
////
////    @Override
////    public short getShort() {
////      if ( getDataType( ) == DataType.INT16 )
////        return reader.readShort( );
////      throw notSupportedError( DataType.INT16.displayName() );
////    }
////
////    @Override
////    public int getInt() {
////      if ( getDataType( ) == DataType.INT32 )
////        return reader.readIntEncoded( );
////      throw notSupportedError( DataType.INT32.displayName() );
////    }
////
////    @Override
////    public long getLong() {
////      if ( getDataType( ) == DataType.INT64 )
////        return reader.readLongEncoded( );
////      throw notSupportedError( DataType.INT64.displayName() );
////    }
////
////    @Override
////    public float getFloat() {
////      if ( getDataType( ) == DataType.FLOAT32 )
////        return reader.readFloat( );
////      throw notSupportedError( DataType.FLOAT32.displayName() );
////    }
////
////    @Override
////    public double getDouble() {
////      if ( getDataType( ) == DataType.FLOAT64 )
////        return reader.readDouble( );
////      throw notSupportedError( DataType.FLOAT64.displayName() );
////    }
////
////    @Override
////    public BigDecimal getDecimal() {
////      if ( getDataType( ) == DataType.DECIMAL )
////        return reader.readDecimal();
////      throw notSupportedError( DataType.DECIMAL.displayName() );
////    }
////
////    @Override
////    public String getString() {
////      if ( getDataType( ) == DataType.STRING )
////        return reader.readString();
////      throw notSupportedError( DataType.STRING.displayName() );
////    }
////
////    @Override
////    public byte[] getBlob() {
////      throw notSupportedError( DataType.BLOB.displayName() );
////    }
////
////    @Override
////    public LocalDate getDate() {
////      throw notSupportedError( DataType.DATE.displayName() );
////    }
////
////    @Override
////    public LocalDateTime getDateTime() {
////      throw notSupportedError( DataType.LOCAL_DATE_TIME.displayName() );
////    }
////
////    @Override
////    public Period getUTCTime() {
////      throw notSupportedError( DataType.UTC_DATE_TIME.displayName() );
////    }
////  }
//  
  int index;
//  private FieldSchema schema;
//  DeserializedTupleAccessor tuple;
  TupleSetDeserializer deserializer;
  TupleReader reader;
//  
  public void bind( TupleSetDeserializer deserializer, int index ) {
    this.deserializer = deserializer;
    this.index = index;
//    tuple = deserializer.tuple;
//    schema = tuple.schema( ).field( index );
    reader = deserializer.reader;
  }
//  
//  @Override
//  public DataType type() {
//    return schema.type();
//  }
//
////  @Override
////  public Cardinality getCardinality() {
////    return schema.getCardinality();
////  }
//
  @Override
  public boolean isNull() {
    return deserializer.isNull( index );
  }
//
////  @Override
////  public ScalarValue asScalar() {
////    throw notSupportedError( "scalar" );
////  }
////
////  @Override
////  public ArrayValue asArray() {
////    throw notSupportedError( "array" );
////  }
////
////  @Override
////  public AnyAccessor asAny() {
////    throw notSupportedError( DataType.ANY.displayName() );
////  }
//
//  public ValueConversionError notSupportedError(String type) {
//    throw new ValueConversionError( "Cannot convert " + schema.getDisplayType( ) +
//                " to " + type );
//  }
//  
//  public void verifyNotNull( ) {
//    if ( isNull( ) )
//      throw new ValueConversionError( "Field is null: " + schema.name( ) );
//  }
  
  protected void seek( ) {
    reader.seek( deserializer.fieldIndexes[ index ] );
  }
//  
//  public static BufferFieldAccessor makeAccessor( FieldSchema schema ) {
//    if ( schema.type() == DataType.LIST ) {
////      JsonItemHandle itemHandle = new JsonItemHandle( handle );
////      JsonFieldAccessor itemAccessor = makeScalarAccessor( itemHandle );
////      return new JsonArrayAccessor( handle, itemAccessor, itemHandle );
//      assert false;
//      return null;
//    }
//    return makeScalarAccessor( schema );
//  }
  
  private static BufferFieldAccessor makeScalarAccessor( FieldSchema schema )
  {
    switch ( schema.type( ) ) {
    case BOOLEAN:
      return new BufferBooleanAccessor( );
    case INT8:
      return new BufferInt8Accessor( );
    case INT16:
      return new BufferInt16Accessor( );
    case INT32:
      return new BufferInt32Accessor( );
    case INT64:
      return new BufferInt64Accessor( );
    case FLOAT32:
      return new BufferFloat32Accessor( );
    case FLOAT64:
      return new BufferFloat64Accessor( );
    case DECIMAL:
      return new BufferDecimalAccessor( );
    case STRING:
      return new BufferStringAccessor( );
//    case ANY: // Any is "any scalar"
//      return new BufferAnyAccessor( );
    default:
      assert false;
      return null;
    }
  }

//  interface BufferFieldHandle
//  {
//    int getIndex( )
//    ValueConversionError notSupportedError( String type );
//    DataType getType();
//    FieldSchema.Cardinality getCardinality();
//  }
//  
//  protected JsonFieldHandle handle;
//
//  protected JsonFieldAccessor( JsonFieldHandle handle ) {
//    this.handle = handle;
//  }
//  
//  @Override
//  public DataType getType() {
//    return handle.getType( );
//  }
//
//  @Override
//  public FieldSchema.Cardinality getCardinality() {
//    return handle.getCardinality( );
//  }
//  
//  @Override
//  public boolean isNull() {
//    return handle.get().getValueType() == ValueType.NULL;
//  }
//
//  @Override
//  public FieldAccessor.ScalarAccessor asScalar() {
//    return null;
//  }
//
//  @Override
//  public FieldAccessor.AnyAccessor asAny() {
//    return null;
//  }
//  
//  @Override
//  public FieldAccessor.ArrayAccessor asArray() {
//    throw handle.notSupportedError( "array" );
//  }
//
//  public static class JsonScalarAccessor extends JsonFieldAccessor implements FieldAccessor.ScalarAccessor
//  {
//    protected JsonScalarAccessor( JsonFieldHandle handle ) {
//      super( handle );
//    }
//    
//    @Override
//    public FieldAccessor.ScalarAccessor asScalar() {
//      return this;
//    }
//
//    @Override
//    public boolean getBoolean() {
//      throw handle.notSupportedError( "boolean" );
//    }
//    
//    @Override
//    public Object getValue() {
//      return handle.get( );
//    }
//
//    @Override
//    public int getInt() {
//      throw handle.notSupportedError( "int" );
//    }
//
//    @Override
//    public long getLong() {
//      throw handle.notSupportedError( "long" );
//    }
//
//    @Override
//    public double getDouble() {
//      throw handle.notSupportedError( "double" );
//    }
//    
//    @Override
//    public BigDecimal getBigDecimal( ) {
//      throw handle.notSupportedError( "BigDouble" );
//    }
//
//    @Override
//    public String getString() {
//      throw handle.notSupportedError( "String" );
//    }
//  }
//  
//  public static class JsonBooleanAccessor extends JsonScalarAccessor
//  {
//    protected JsonBooleanAccessor( JsonFieldHandle handle ) {
//      super( handle );
//    }
//    
//    @Override
//    public boolean getBoolean() {
//      return handle.get().getValueType() == ValueType.TRUE;
//    }
//  }
//      
//  public static class JsonNumberAccessor extends JsonScalarAccessor
//  {
//    protected JsonNumberAccessor( JsonFieldHandle handle ) {
//      super( handle );
//    }
//    
//    @Override
//    public int getInt() {
//      try {
//        return ((JsonNumber) handle.get( )).intValueExact( );
//      }
//      catch ( Exception e ) {
//        throw handle.notSupportedError( "int" );
//      }
//    }
//
//    @Override
//    public long getLong() {
//      try {
//        return ((JsonNumber) handle.get( )).longValueExact( );
//      }
//      catch ( Exception e ) {
//        throw handle.notSupportedError( "long" );
//      }
//    }
//
//    @Override
//    public double getDouble() {
//      try {
//        return ((JsonNumber) handle.get( )).doubleValue( );
//      }
//      catch ( Exception e ) {
//        throw handle.notSupportedError( "double" );
//      }
//    }
//    
//    @Override
//    public BigDecimal getBigDecimal( ) {
//      try {
//        return ((JsonNumber) handle.get( )).bigDecimalValue();
//      }
//      catch ( Exception e ) {
//        throw handle.notSupportedError( "BigDecimal" );
//      }
//    }
//  }
//  
//  public static class JsonStringAccessor extends JsonScalarAccessor
//  {
//    protected JsonStringAccessor( JsonFieldHandle handle ) {
//      super( handle );
//    }
//    
//    @Override
//    public String getString() {
//      try {
//        return ((JsonString) handle.get( )).getString();
//      }
//      catch ( Exception e ) {
//        throw handle.notSupportedError( "String" );
//      }
//    }
//  }
//  
//  public static class JsonAnyAccessor extends JsonNumberAccessor implements FieldAccessor.AnyAccessor
//  {
//    protected JsonAnyAccessor( JsonFieldHandle handle ) {
//      super( handle );
//    }
//    
//    @Override
//    public FieldAccessor.AnyAccessor asAny() {
//      return this;
//    }  
//    
//    @Override
//    public boolean getBoolean() {
//      JsonValue value = handle.get();
//      if ( value.getValueType() == ValueType.TRUE )
//        return true;
//      if ( value.getValueType() == ValueType.FALSE )
//        return false;
//      throw handle.notSupportedError( "Boolean" );
//    }
//    
//    @Override
//    public String getString() {
//      try {
//        return ((JsonString) handle.get( )).getString();
//      }
//      catch ( Exception e ) {
//        throw handle.notSupportedError( "String" );
//      }
//    }
//
//    @Override
//    public DataType getDataType() {
//      return JsonSchemaBuilder.inferScalarType( handle.get( ) );
//    }
//  }
//  
//  public static class JsonItemHandle implements JsonFieldHandle
//  {
//    int index;
//    private JsonFieldHandle arrayHandle;
//    
//    public JsonItemHandle( JsonFieldHandle arrayHandle ) {
//      this.arrayHandle = arrayHandle;
//    }
//    
//    void setIndex( int index ) {
//      this.index = index;
//    }
//
//    @Override
//    public JsonValue get() {
//      JsonValue value = arrayHandle.get();
//      assert value.getValueType() == ValueType.ARRAY;
//      JsonArray array =  (JsonArray) value;
//      if ( index < 0  ||  index >= array.size() )
//        return null;
//      return array.get( index );
//    }
//
//    @Override
//    public ValueConversionError notSupportedError(String type) {
//      return new ValueConversionError( "Cannot convert " + arrayHandle.getType().toString() +
//                  "[" + index + "] to " + type );
//    }
//
//    @Override
//    public DataType getType() {
//      return arrayHandle.getType();
//    }
//
//    @Override
//    public FieldSchema.Cardinality getCardinality() {
//      return FieldSchema.Cardinality.Required;
//    }
//  }
//  
//  public static class JsonArrayAccessor extends JsonFieldAccessor implements FieldAccessor.ArrayAccessor
//  {
//    private JsonFieldAccessor valueAccessor;
//    private JsonItemHandle valueHandle;
//    
//    protected JsonArrayAccessor(JsonFieldHandle handle, JsonFieldAccessor valueAccessor, JsonItemHandle valueHandle) {
//      super(handle);
//      this.valueHandle = valueHandle;
//      this.valueAccessor = valueAccessor;
//    }
//
//    @Override
//    public FieldAccessor.ArrayAccessor asArray() {
//      return this;
//    }
//    
//    private JsonArray getJsonArray( ) {
//      return (JsonArray) handle.get( );
//    }
//
//    @Override
//    public int size() {
//      return getJsonArray( ).size();
//    }
//
//    @Override
//    public FieldAccessor get(int i) {
//      if ( i < 0  ||  i >= size( ) )
//        return null;
//      valueHandle.setIndex( i );
//      return valueAccessor;
//    }
//
//    @Override
//    public DataType getValueType() {
//      return handle.getType();
//    }
//
//    @Override
//    public FieldSchema.Cardinality getValueCardinality() {
//      return FieldSchema.Cardinality.Optional;
//    }
//
//    @SuppressWarnings("unchecked")
//    @Override
//    public <V> V[] toArray() {
//      V array[] = (V[]) new Object[ size( ) ];
//      for ( int i = 0;  i < array.length;  i++ ) {
//        array[i] = (V) get( i ).asScalar().getValue();
//      }
//      return array;
//    }
//
//    @Override
//    public int[] toIntArray() {
//      int array[] = new int[ size( ) ];
//      for ( int i = 0;  i < array.length;  i++ ) {
//        array[i] = get( i ).asScalar().getInt();
//      }
//      return null;
//    }
//    
//  }

//  public class JsonMapAccessor extends JsonFieldAccessor implements MapAccessor
//  {
//    JsonItemHandle keyHandle = new JsonItemHandle( );
//    JsonFieldAccessor keyAccessor;
//    JsonItemHandle valueHandle = new JsonItemHandle( );
//    JsonFieldAccessor valueAccessor;
//    
//    protected JsonMapAccessor(JsonFieldHandle handle) {
//      super(handle);
//    }
//
//    @Override
//    public MapAccessor asMap() {
//      return this;
//    }
//    
//    private JsonObject getJsonObject( ) {
//      return (JsonObject) handle.get( );
//    }
//
//    @Override
//    public int size() {
//      return getJsonObject( ).size();
//    }
//
////    @Override
////    public Iterable<String> getStringKeys() {
////      return getJsonObject( ).keySet();
////    }
////
////    @Override
////    public FieldAccessor get(String key) {
////      return getJsonObject( ).get( key );
////    }
//
//    @Override
//    public FieldType getKeyType() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public FieldType getValueType() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public FieldCardinality getValueCardinality() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public <K> K getKey(int i) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public <V> V getValue(int i) {
//      // TODO Auto-generated method stub
//      return null;
//    }
//
//    @Override
//    public <K, V> Map<K, V> toMap() {
//      // TODO Auto-generated method stub
//      return null;
//    }
//    
//  }
  
}
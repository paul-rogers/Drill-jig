package org.apache.drill.jig.extras.json;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.ScalarValue;
import org.apache.drill.jig.exception.ValueConversionError;

public class JsonFieldAccessor implements FieldValue
{
  interface JsonFieldHandle
  {
    JsonValue get( );
    ValueConversionError notSupportedError( String type );
    DataType getType();
    Cardinality getCardinality();
  }
  
  protected JsonFieldHandle handle;

  protected JsonFieldAccessor( JsonFieldHandle handle ) {
    this.handle = handle;
  }
  
  @Override
  public DataType type() {
    return handle.getType( );
  }

  @Override
  public Cardinality getCardinality() {
    return handle.getCardinality( );
  }
  
  @Override
  public boolean isNull() {
    return handle.get().getValueType() == ValueType.NULL;
  }

  @Override
  public ScalarValue asScalar() {
    return null;
  }

  @Override
  public FieldValue.AnyAccessor asAny() {
    return null;
  }
  
  @Override
  public ArrayValue asArray() {
    throw handle.notSupportedError( "array" );
  }

  public static class JsonScalarAccessor extends JsonFieldAccessor implements ScalarValue
  {
    protected JsonScalarAccessor( JsonFieldHandle handle ) {
      super( handle );
    }
    
    @Override
    public ScalarValue asScalar() {
      return this;
    }

    @Override
    public boolean getBoolean() {
      throw notSupportedError( DataType.BOOLEAN.displayName() );
    }   

    @Override
    public byte getByte() {
      throw notSupportedError( DataType.INT8.displayName() );
    }

    @Override
    public int getInt() {
      throw notSupportedError( DataType.INT32.displayName( ) );
    }

    @Override
    public short getShort() {
      throw notSupportedError( DataType.INT16.displayName() );
    }

    @Override
    public long getLong() {
      throw notSupportedError( DataType.INT64.displayName() );
    }

    @Override
    public float getFloat() {
      throw notSupportedError( DataType.STRING.displayName() );
    }

    @Override
    public double getDouble() {
      throw notSupportedError( DataType.FLOAT64.displayName() );
    }

    @Override
    public BigDecimal getDecimal() {
      throw notSupportedError( DataType.DECIMAL.displayName() );
    }

    @Override
    public String getString() {
      throw notSupportedError( DataType.STRING.displayName() );
    }

    @Override
    public byte[] getBlob() {
      throw notSupportedError( DataType.BLOB.displayName() );
    }

    @Override
    public LocalDate getDate() {
      throw notSupportedError( DataType.DATE.displayName() );
    }

    @Override
    public LocalDateTime getDateTime() {
      throw notSupportedError( DataType.LOCAL_DATE_TIME.displayName() );
    }

    @Override
    public Period getUTCTime() {
      throw notSupportedError( DataType.UTC_DATE_TIME.displayName() );
    }

    @Override
    public Object getValue() {
      throw notSupportedError( "Object" );
    }
    
    public ValueConversionError notSupportedError(String type) {
      return handle.notSupportedError( type );
    }
  }
  
  public static class JsonBooleanAccessor extends JsonScalarAccessor
  {
    protected JsonBooleanAccessor( JsonFieldHandle handle ) {
      super( handle );
    }
    
    @Override
    public boolean getBoolean() {
      return handle.get().getValueType() == ValueType.TRUE;
    }
  }
      
  public static class JsonNumberAccessor extends JsonScalarAccessor
  {
    protected JsonNumberAccessor( JsonFieldHandle handle ) {
      super( handle );
    }
    
    @Override
    public int getInt() {
      try {
        return ((JsonNumber) handle.get( )).intValueExact( );
      }
      catch ( Exception e ) {
        throw handle.notSupportedError( "int" );
      }
    }

    @Override
    public long getLong() {
      try {
        return ((JsonNumber) handle.get( )).longValueExact( );
      }
      catch ( Exception e ) {
        throw handle.notSupportedError( "long" );
      }
    }

    @Override
    public double getDouble() {
      try {
        return ((JsonNumber) handle.get( )).doubleValue( );
      }
      catch ( Exception e ) {
        throw handle.notSupportedError( "double" );
      }
    }
    
    @Override
    public BigDecimal getDecimal( ) {
      try {
        return ((JsonNumber) handle.get( )).bigDecimalValue();
      }
      catch ( Exception e ) {
        throw handle.notSupportedError( "BigDecimal" );
      }
    }
  }
  
  public static class JsonStringAccessor extends JsonScalarAccessor
  {
    protected JsonStringAccessor( JsonFieldHandle handle ) {
      super( handle );
    }
    
    @Override
    public String getString() {
      try {
        return ((JsonString) handle.get( )).getString();
      }
      catch ( Exception e ) {
        throw handle.notSupportedError( "String" );
      }
    }
  }
  
  public static class JsonAnyAccessor extends JsonNumberAccessor implements FieldValue.AnyAccessor
  {
    protected JsonAnyAccessor( JsonFieldHandle handle ) {
      super( handle );
    }
    
    @Override
    public FieldValue.AnyAccessor asAny() {
      return this;
    }  
    
    @Override
    public FieldValue.AnyAccessor asScalar() {
      return this;
    }  
    
    @Override
    public boolean getBoolean() {
      JsonValue value = handle.get();
      if ( value.getValueType() == ValueType.TRUE )
        return true;
      if ( value.getValueType() == ValueType.FALSE )
        return false;
      throw handle.notSupportedError( "Boolean" );
    }
    
    @Override
    public String getString() {
      try {
        return ((JsonString) handle.get( )).getString();
      }
      catch ( Exception e ) {
        throw handle.notSupportedError( "String" );
      }
    }

    @Override
    public DataType getDataType() {
      return JsonSchemaBuilder.inferScalarType( handle.get( ) );
    }
  }
  
  public static class JsonItemHandle implements JsonFieldHandle
  {
    int index;
    private JsonFieldHandle arrayHandle;
    
    public JsonItemHandle( JsonFieldHandle arrayHandle ) {
      this.arrayHandle = arrayHandle;
    }
    
    void setIndex( int index ) {
      this.index = index;
    }

    @Override
    public JsonValue get() {
      JsonValue value = arrayHandle.get();
      assert value.getValueType() == ValueType.ARRAY;
      JsonArray array =  (JsonArray) value;
      if ( index < 0  ||  index >= array.size() )
        return null;
      return array.get( index );
    }

    @Override
    public ValueConversionError notSupportedError(String type) {
      return new ValueConversionError( "Cannot convert " + arrayHandle.getType().toString() +
                  "[" + index + "] to " + type );
    }

    @Override
    public DataType getType() {
      return arrayHandle.getType();
    }

    @Override
    public Cardinality getCardinality() {
      return Cardinality.REQUIRED;
    }
  }
  
  public static class JsonArrayAccessor extends JsonFieldAccessor implements ArrayValue
  {
    private JsonFieldAccessor valueAccessor;
    private JsonItemHandle valueHandle;
    
    protected JsonArrayAccessor(JsonFieldHandle handle, JsonFieldAccessor valueAccessor, JsonItemHandle valueHandle) {
      super(handle);
      this.valueHandle = valueHandle;
      this.valueAccessor = valueAccessor;
    }

    @Override
    public ArrayValue asArray() {
      return this;
    }
    
    private JsonArray getJsonArray( ) {
      return (JsonArray) handle.get( );
    }

    @Override
    public int size() {
      return getJsonArray( ).size();
    }

    @Override
    public FieldValue get(int i) {
      if ( i < 0  ||  i >= size( ) )
        return null;
      valueHandle.setIndex( i );
      return valueAccessor;
    }

    @Override
    public DataType memberType() {
      return handle.getType();
    }

    @Override
    public Cardinality getValueCardinality() {
      return Cardinality.OPTIONAL;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> V[] toArray() {
      V array[] = (V[]) new Object[ size( ) ];
      for ( int i = 0;  i < array.length;  i++ ) {
        array[i] = (V) get( i ).asScalar().getValue();
      }
      return array;
    }

    @Override
    public int[] toIntArray() {
      int array[] = new int[ size( ) ];
      for ( int i = 0;  i < array.length;  i++ ) {
        array[i] = get( i ).asScalar().getInt();
      }
      return null;
    }
    
  }

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
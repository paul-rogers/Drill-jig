package org.apache.drill.jig.extras.json;

import java.math.BigDecimal;
import java.util.Collection;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.extras.json.JsonTupleSet.JsonTupleValue;
import org.apache.drill.jig.types.CachedObjectAccessor;
import org.apache.drill.jig.types.FieldAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.FieldAccessor.BooleanAccessor;
import org.apache.drill.jig.types.FieldAccessor.DecimalAccessor;
import org.apache.drill.jig.types.FieldAccessor.Int64Accessor;
import org.apache.drill.jig.types.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldAccessor.StringAccessor;
import org.apache.drill.jig.types.FieldAccessor.TypeAccessor;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.VariantFieldValueContainer;

/**
 * <ul>
 * <li>JsonObjectAccessor: Provides access to a nested or top-level Json object.</li>
 * <li>JsonMemberAccessor: Holds a JsonObjectAcccessor and a key; provides access to
 * a member value within the object.</li>
 * <li>JsonValueAccessor: Given an object accessor, implements various scalar get
 * methods that convert from JSON values to primitives.</li>
 * <li>JsonArrayAccessor: Given an object accessor, provides methods to work with
 * a JSON array using the Jig API.<li>
 * <li>JsonArrayMemberAccessor: Given a JsonArray accessor, binds to an index at
 * runtime to provide access to array members.</li>
 * </ul>
 */

public class JsonAccessor {

//  public interface PushedObjectAccessor {
//    void push( Object object );
//  }
//  
//  public interface JsonObjectAccessor extends FieldAccessor {
//    JsonObject getJsonObject( );
//  }
//  
//  public class CachedJsonObjectAccessor implements JsonObjectAccessor {
//    private JsonObject value;
//
//    public void bind( JsonObject value ) {
//      this.value = value;
//    }
//    
//    public JsonObject getJsonObject( ) {
//      return value;
//    }
//
//    @Override
//    public boolean isNull() {
//      return checkNull( value );
//    }
//  }
//  
//  public interface JsonValueAccessor extends FieldAccessor {
//    JsonValue getJsonValue( );
//  }
//  
//  public class CachedJsonValueAcccessor implements JsonValueAccessor {
//    private JsonValue value;
//
//    public void bind( JsonValue value ) {
//      this.value = (value == null) ? JsonValue.NULL : value;
//    }
//    
//    public JsonValue getJsonValue( ) {
//      return value;
//    }
//
//    @Override
//    public boolean isNull() {
//      return checkNull( value );
//    }
//  }

  public static class TupleObjectAccessor implements ObjectAccessor {

    private JsonTupleValue tupleValue;

    void bind( JsonTupleValue tupleValue ) {
      this.tupleValue = tupleValue;
    }
    
    @Override
    public boolean isNull() {
      return false;
    }

    @Override
    public Object getObject() {
      return tupleValue.getJsonObject( );
    }
  }

  public static class JsonMapAccessor implements MapValueAccessor, MapValue {
    
    private ObjectAccessor accessor;
    private VariantFieldValueContainer container;
    CachedObjectAccessor memberAccessor = new CachedObjectAccessor( );

    public JsonMapAccessor( ObjectAccessor accessor, FieldValueFactory factory ) {
      this.accessor = accessor;
      container = new VariantFieldValueContainer( factory );
      container.bind( new JsonValueAccessor( memberAccessor ) );
    }

    @Override
    public boolean isNull() {
      return accessor.isNull( );
    }

    @Override
    public MapValue getMap() {
      return this;
    }

    @Override
    public Object getValue() {
      return accessor.getObject();
    }
    
    private JsonObject getJsonObject( ) {
      return (JsonObject) getValue( );
    }
    
    @Override
    public int size() {
      return getJsonObject( ).size();
    }

    @Override
    public Collection<String> keys() {
      return getJsonObject( ).keySet();
    }

    @Override
    public FieldValue get(String key) {
      memberAccessor.bind( getJsonObject( ).get( key ) );
      return container.get();
    }
    
  }
  public static class JsonValueAccessor implements StringAccessor, DecimalAccessor, Int64Accessor, BooleanAccessor, TypeAccessor {

    private ObjectAccessor accessor;

    public JsonValueAccessor( ObjectAccessor accessor ) {
      this.accessor = accessor;
    }
    
    @Override
    public boolean isNull() {
      return checkNull( accessor.getObject() );
    }
    
    private Object getValue( ) {
      return accessor.getObject();
    }

    @Override
    public boolean getBoolean() {
      return getValue( ) == JsonValue.TRUE;
    }

    @Override
    public long getLong() {
      return ((JsonNumber) getValue( )).longValueExact();
    }

    @Override
    public BigDecimal getDecimal() {
      return ((JsonNumber) getValue( )).bigDecimalValue();
    }

    @Override
    public String getString() {
       return ((JsonString) getValue()).getString();
    }

    @Override
    public DataType getType() {
      return ObjectParser.parseType( (JsonValue) accessor.getObject() );
    }
  }

  public static class JsonObjectMemberAccessor implements ObjectAccessor {
    
    private final ObjectAccessor accessor;
    private final String key;

    public JsonObjectMemberAccessor( ObjectAccessor accessor, String key ) {
      this.accessor = accessor;
      this.key = key;
    }

    @Override
    public boolean isNull() {
      return checkNull( getJsonValue( ) );
    }

    public JsonValue getJsonValue() {
      JsonObject object = (JsonObject) accessor.getObject();
      if ( object == null )
        return JsonValue.NULL;
      JsonValue value = object.get( key );
      if ( value == null )
        return JsonValue.NULL;
      return value;
    }

    @Override
    public Object getObject() {
      return getJsonValue( );
    }
  }

  public static class JsonArrayAccessor implements ArrayAccessor {

    protected class MemberAccessor implements IndexedAccessor, ObjectAccessor
    {
      protected int index;
      
      @Override
      public void bind(int index) {
        this.index = index;
      }
      
      @Override
      public boolean isNull() {
        return checkNull( getObject( ) );
      }

      @Override
      public Object getObject() {
         return getArray( ).get(index);
      }
    }

    private ObjectAccessor accessor;
    private final MemberAccessor memberAccessor = new MemberAccessor( );

    public JsonArrayAccessor( ObjectAccessor accessor ) {
      this.accessor = accessor;
    }

    @Override
    public boolean isNull() {
      return checkNull( getValue( ) );
    }
    
    protected JsonArray getArray( ) {
      Object array = accessor.getObject( );
      if ( checkNull( array ) )
        return null;
      return (JsonArray) array;
    }
    
    @Override
    public FieldAccessor memberAccessor() {
      return memberAccessor;
    }

    @Override
    public int size() {
      return getArray( ).size();
    }

    @Override
    public Object getValue() {
      return accessor.getObject();
    }

    @Override
    public void select(int index) {
      if ( index < 0  ||  size( ) <= index )
        throw new ArrayIndexOutOfBoundsException( );
      memberAccessor.bind( index );
    }
  }

  public static boolean checkNull( Object value ) {
    return value == null  ||  value == JsonValue.NULL;
  }
}

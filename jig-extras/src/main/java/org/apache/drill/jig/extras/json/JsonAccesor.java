package org.apache.drill.jig.extras.json;

import java.math.BigDecimal;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.types.FieldAccessor;
import org.apache.drill.jig.types.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.types.FieldAccessor.BooleanAccessor;
import org.apache.drill.jig.types.FieldAccessor.DecimalAccessor;
import org.apache.drill.jig.types.FieldAccessor.Int64Accessor;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldAccessor.StringAccessor;
import org.apache.drill.jig.types.FieldAccessor.TypeAccessor;

public class JsonAccesor {

  public static class JsonValueAccessor implements StringAccessor, DecimalAccessor, Int64Accessor, BooleanAccessor, TypeAccessor {

    private ObjectAccessor accessor;

    public JsonValueAccessor( ObjectAccessor accessor ) {
      this.accessor = accessor;
    }
    
    @Override
    public boolean isNull() {
      return checkNull( getValue( ) );
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
      return ObjectParser.parseType( (JsonValue) getValue( ) );
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
      return checkNull( getObject( ) );
    }

    @Override
    public Object getObject() {
      JsonObject object = (JsonObject) accessor.getObject();
      if ( object == null )
        return null;
      return object.get( key );
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
      Object value = getValue( );
      return value == null  ||  value == JsonValue.NULL; 
    }
    
    protected JsonArray getArray( ) {
      return (JsonArray) accessor.getObject( );
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

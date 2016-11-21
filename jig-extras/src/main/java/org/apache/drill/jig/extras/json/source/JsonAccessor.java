package org.apache.drill.jig.extras.json.source;

import java.math.BigDecimal;
import java.util.Collection;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.drill.jig.accessor.CachedObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ArrayAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.BooleanAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.DecimalAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.Int64Accessor;
import org.apache.drill.jig.accessor.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.StringAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.TypeAccessor;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.container.VariantFieldValueContainer;
import org.apache.drill.jig.extras.json.source.JsonTupleSet.JsonTupleValue;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.util.JigUtilities;
import org.apache.drill.jig.util.Visualizable;

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

public class JsonAccessor implements Visualizable {

  public static class TupleObjectAccessor extends JsonAccessor implements ObjectAccessor {

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

  public static class JsonMapAccessor extends JsonAccessor implements MapValueAccessor, MapValue {

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

    private JsonObject getJsonObject( ) {
      return (JsonObject) accessor.getObject();
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

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      JigUtilities.visualizeLn(buf, indent + 1, "accessor", accessor);
      JigUtilities.visualizeLn(buf, indent + 1, "member accessor", memberAccessor);
      JigUtilities.visualizeLn(buf, indent + 1, "container", container);
      JigUtilities.indent(buf, indent + 1 );
      buf.append( "]" );
    }
  }

  public static class JsonValueAccessor extends JsonAccessor implements StringAccessor, DecimalAccessor, Int64Accessor, BooleanAccessor, TypeAccessor {

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

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      JigUtilities.visualizeLn(buf, indent + 1, "accessor", accessor);
      JigUtilities.indent(buf, indent + 1 );
      buf.append( "]" );
    }
  }

  public static class JsonObjectMemberAccessor extends JsonAccessor implements ObjectAccessor {

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

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      buf.append( " key = " );
      buf.append( key );
      JigUtilities.visualizeLn(buf, indent + 1, "accessor", accessor);
      JigUtilities.indent(buf, indent + 1 );
      buf.append( "]" );
    }
  }

  public static class JsonArrayAccessor extends JsonAccessor implements ArrayAccessor {

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

      @Override
      public void visualize(StringBuilder buf, int indent) {
        JigUtilities.objectHeader( buf, this );
        buf.append( "]" );
      }
    }

    private ObjectAccessor accessor;
    private final MemberAccessor memberAccessor = new MemberAccessor( );

    public JsonArrayAccessor( ObjectAccessor accessor ) {
      this.accessor = accessor;
    }

    @Override
    public boolean isNull() {
      return checkNull( accessor.getObject() );
    }

    protected JsonArray getArray( ) {
      Object array = accessor.getObject( );
      if ( checkNull( array ) )
        return null;
      return (JsonArray) array;
    }

    @Override
    public FieldAccessor elementAccessor() {
      return memberAccessor;
    }

    @Override
    public int size() {
      return getArray( ).size();
    }

    @Override
    public void select(int index) {
      if ( index < 0  ||  size( ) <= index )
        throw new ArrayIndexOutOfBoundsException( );
      memberAccessor.bind( index );
    }

    @Override
    public void visualize(StringBuilder buf, int indent) {
      JigUtilities.objectHeader( buf, this );
      JigUtilities.visualizeLn(buf, indent + 1, "accessor", accessor);
      JigUtilities.visualizeLn(buf, indent + 1, "member accessor", memberAccessor);
      JigUtilities.indent(buf, indent + 1 );
      buf.append( "]" );
    }
  }

  public static boolean checkNull( Object value ) {
    return value == null  ||  value == JsonValue.NULL;
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "]" );
  }
}

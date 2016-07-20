package org.apache.drill.jig.api.json;

import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldAccessor;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.api.json.JsonFieldAccessor.JsonAnyAccessor;
import org.apache.drill.jig.api.json.JsonFieldAccessor.JsonArrayAccessor;
import org.apache.drill.jig.api.json.JsonFieldAccessor.JsonBooleanAccessor;
import org.apache.drill.jig.api.json.JsonFieldAccessor.JsonFieldHandle;
import org.apache.drill.jig.api.json.JsonFieldAccessor.JsonItemHandle;
import org.apache.drill.jig.api.json.JsonFieldAccessor.JsonNumberAccessor;
import org.apache.drill.jig.api.json.JsonFieldAccessor.JsonStringAccessor;


public class JsonTupleSchema extends TupleSchemaImpl
{
  protected static class JsonFieldSchema extends FieldSchemaImpl
  {
    protected final JsonTupleSchema tupleSchema;
    protected JsonFieldSchema parent;

    public JsonFieldSchema( String name, DataType type, Cardinality cardinality, JsonTupleSchema tuple ) {
      super( name, type, cardinality );
      this.tupleSchema = tuple;
    }
    
    public JsonFieldSchema(String name, FieldSchema field, JsonFieldSchema parent) {
      super( name, field.getType( ), field.getCardinality( ) );
      tupleSchema = null;
      this.parent = parent;
    }

//    @Override
//    public TupleSchema getStructure() {
//      return tupleSchema;
//    }

    public void becomeAny() {
      type = DataType.ANY;
    }

    public boolean isCompatible(JsonValue value) {
      ValueType vType = value.getValueType();
      if ( type == DataType.MAP || vType == ValueType.OBJECT ) {
        if ( type != DataType.MAP  ||  vType != ValueType.OBJECT ) {
          return false;
        }
        return tupleSchema.isCompatible( (JsonObject) value );
      }
      if ( cardinality == Cardinality.REPEATED ||  vType == ValueType.ARRAY ) {
        return ( cardinality == Cardinality.REPEATED  &&  vType == ValueType.ARRAY );
      }
      DataType dataType = JsonSchemaBuilder.inferScalarType( value );
      return type.isCompatible( dataType );
    }

    public FieldAccessor makeAccessor(JsonFieldHandle handle) {
      if ( cardinality == Cardinality.REPEATED ) {
        JsonItemHandle itemHandle = new JsonItemHandle( handle );
        JsonFieldAccessor itemAccessor = makeScalarAccessor( itemHandle );
        return new JsonArrayAccessor( handle, itemAccessor, itemHandle );
      }
      return makeScalarAccessor( handle );
    }
    
    private JsonFieldAccessor makeScalarAccessor( JsonFieldHandle handle )
    {
      switch ( type ) {
      case BOOLEAN:
        return new JsonBooleanAccessor( handle );
      case DECIMAL:
      case FLOAT64:
      case INT64:
        return new JsonNumberAccessor( handle );
      case STRING:
        return new JsonStringAccessor( handle );
      case ANY: // Any is "any scalar"
        return new JsonAnyAccessor( handle );
      default:
        return new JsonFieldAccessor( handle );
      }
    }

  }
  
//  protected List<JsonFieldSchema> fields = new ArrayList<>( );
//  protected Map<String,JsonFieldSchema> nameIndex = new HashMap<>( );
  protected boolean isFlattened;
  
  public JsonTupleSchema() {
  }

//  @Override
//  public int getCount() {
//    return fields.size();
//  }
//
//  @Override
//  public FieldSchema getField(int i) {
//    if ( i < 0  &&  i >= fields.size( ) ) {
//      return null; }
//    return fields.get( i );
//  }
//
//  @Override
//  public FieldSchema getField(String name) {
//    return nameIndex.get( name );
//  }

  public void add(JsonFieldSchema field) {
    super.add( field );
//    field.index = fields.size( );
//    fields.add( field );
//    if ( nameIndex.containsKey( field.name ) ) {
//      throw new JsonScannerException( "Duplicate field name: " + field.name );
//    }
//    nameIndex.put( field.name, field );
    if ( field.getName( ).contains( "." ) )
      isFlattened = true;
  }
  
  public boolean isCompatible( JsonObject object ) {
    return isCompatible( object, null );
  }
    
  public boolean isCompatible( JsonObject object, String path ) {
    for ( String key : object.keySet() ) {
      JsonFieldSchema field = (JsonFieldSchema) getField( key );
      if ( field == null )
        return false;
      JsonValue value = object.get( key );
      boolean compat = true;
      if ( value.getValueType() == ValueType.OBJECT ) {
        if ( isFlattened ) {
          String newPrefix;
          if ( path == null )
            newPrefix = path + "." + key;
          else
            newPrefix = key;
          compat = isCompatible( object, newPrefix );
        }
        else {
          compat = field.tupleSchema.isCompatible( (JsonObject) value );
        }
      }
      else {
        compat = field.isCompatible( value );
      }
      if ( ! compat ) {
        return false;
      }
    }
    return true;
  }
  
  public void merge(JsonTupleSchema previous) {
    for ( FieldSchema oldField : previous.fields( ) ) {
      String key = oldField.getName();
      JsonFieldSchema newField = (JsonFieldSchema) getField( key );
      if ( newField == null ) {
        add( (JsonFieldSchema) oldField );
      }
      if ( oldField.getType( ).isScalar() &&  newField.getType( ).isScalar( ) ) {
        if ( oldField.getType( ) != newField.getType( ) ) {
          newField.becomeAny( );
        }
      }
      else if ( oldField.getType( ) == DataType.MAP  &&  newField.getType() == DataType.MAP ) {
        newField.tupleSchema.merge( ((JsonFieldSchema) oldField).tupleSchema );
      }
    }
  }
  
  public JsonTupleSchema flatten( ) {
    JsonTupleSchema flattened = new JsonTupleSchema( );
    buildFlattened( flattened, null );
    return flattened;
  }

  public void buildFlattened( JsonTupleSchema flattened, JsonFieldSchema parent ) {
    for ( FieldSchema field : fields( ) ) {
      String name;
      if ( parent == null ) {
        name = field.getName( );
      }
      else {
        name = parent.getName( ) + "." + field.getName();
      }
      JsonFieldSchema jsonField = (JsonFieldSchema) field;
      if ( field.getType() == DataType.MAP ) {
        jsonField.tupleSchema.buildFlattened( flattened, jsonField );
      }
      else {
        flattened.add( new JsonFieldSchema( name, jsonField, parent ) );
      }
    }
  }
  
}

package org.apache.drill.jig.extras.json;

import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.exception.ValueConversionError;
import org.apache.drill.jig.extras.json.JsonFieldAccessor.JsonFieldHandle;
import org.apache.drill.jig.extras.json.JsonTupleSchema.JsonFieldSchema;
import org.apache.drill.jig.extras.json.reader.BufferingTupleReader;
import org.apache.drill.jig.extras.json.reader.JsonScannerException;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;

public class JsonTupleSet implements TupleSet, TupleValue
{
  private enum State { START, RUN, EOF, SCHEMA_CHANGE };
  
//  public class TupleFieldHandle implements JsonFieldHandle
//  {
//    protected final JsonFieldSchema fieldSchema;
//    
//    protected TupleFieldHandle( JsonFieldSchema schema ) {
//      this.fieldSchema = schema;
//    }
//    
//    @Override
//    public JsonValue get( ) {
//      return currentObject.get( fieldSchema.name() );
//    }
//
//    @Override
//    public ValueConversionError notSupportedError( String type ) {
//      return new ValueConversionError( "Cannot convert " + fieldSchema.type().toString() + " to " + type );
//    }
//
//    @Override
//    public DataType getType() {
//      return fieldSchema.type();
//    }
//
//    @Override
//    public Cardinality getCardinality() {
//      return fieldSchema.getCardinality();
//    }
//
//  }

  public class TupleObjectAccessor implements ObjectAccessor {

    @Override
    public boolean isNull() {
      return currentObject == null;
    }

    @Override
    public Object getObject() {
      return currentObject;
    }
  }

  private final BufferingTupleReader recordReader;
  JsonTupleSchema resultSchema;
  JsonTupleSchema inputSchema;
  private State state = State.START;
  private JsonObject currentObject;
  private FieldValue fieldAccessors[];
  
  public JsonTupleSet(BufferingTupleReader recordReader ) {
    this.recordReader = recordReader;
  }
  
  public void buildSchema( JsonObject object ) {
    JsonSchemaBuilder builder = new JsonSchemaBuilder( );
    inputSchema = builder.build( object );
    makeResultSchema( );
  }
  
  private void makeResultSchema( )
  {
    resultSchema = inputSchema.flatten( );
    fieldAccessors = buildAccessors( );
  }

  public void mergeSchema( JsonTupleSchema prevSchema, JsonObject object ) {
    JsonSchemaBuilder builder = new JsonSchemaBuilder( );
    inputSchema = builder.build( object );
    inputSchema.merge( prevSchema );
    makeResultSchema( );
  }

  @Override
  public TupleSchema schema() {
    return resultSchema;
  }

  @Override
  public int getIndex() {
    return recordReader.getIndex();
  }
  
  protected boolean isDone( ) {
    return state == State.EOF ||
           state == State.SCHEMA_CHANGE;
  }
  
  protected boolean hasSchemaChange( ) {
    return state == State.SCHEMA_CHANGE;
  }

  @Override
  public boolean next() {
    JsonObject obj = recordReader.next( );
    if ( obj == null ) {
      state = State.EOF;
      currentObject = null;
      return false;
    }
    if ( obj.getValueType() != ValueType.OBJECT ) {
      throw new JsonScannerException( "Object is required but " + obj.getValueType() + " found." );
    }
    if ( ! inputSchema.isCompatible( obj ) ) {
      recordReader.push( obj );
      state = State.SCHEMA_CHANGE;
      return false;
    }
    currentObject = obj;
    return true;
  }

  @Override
  public TupleValue getTuple() {
    return this;
  }

  public void inferSchema() {
    JsonObject obj = recordReader.next();
    buildSchema( obj );
    recordReader.push( obj );
  }

  private FieldValue[] buildAccessors( ) {
    FieldValue accessors[] = new FieldValue[ resultSchema.count( ) ];
    for ( int i = 0;  i < accessors.length;  i++ ) {
      JsonFieldSchema field = (JsonFieldSchema) resultSchema.field( i );
      accessors[i] = field.makeAccessor( new TupleFieldHandle( field ) );
    }
    return accessors;
  }

  public void evolveSchema(JsonTupleSchema previous) {
    JsonObject obj = recordReader.next();
    mergeSchema( previous, obj );
    recordReader.push( obj );
  }

  @Override
  public FieldValue field(int i) {
    if ( i < 0  &&  i >= fieldAccessors.length ) {
      return null; }
    return fieldAccessors[ i ];
  }

  @Override
  public FieldValue field(String name) {
    FieldSchema field = resultSchema.field( name );
    if ( field == null ) {
      return null;
    }
    return fieldAccessors[ field.index() ];
  }
  
  public boolean isEOF() {
    return state == State.EOF;
  }   
}

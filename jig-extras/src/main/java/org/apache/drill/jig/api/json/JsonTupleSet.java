package org.apache.drill.jig.api.json;

import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldAccessor;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleAccessor;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.ValueConversionError;
import org.apache.drill.jig.api.json.JsonFieldAccessor.JsonFieldHandle;
import org.apache.drill.jig.api.json.JsonScanner.BufferingTupleReader;
import org.apache.drill.jig.api.json.JsonTupleSchema.JsonFieldSchema;

public class JsonTupleSet implements TupleSet, TupleAccessor
{
  private enum State { START, RUN, EOF, SCHEMA_CHANGE };
  
  public class TupleFieldHandle implements JsonFieldHandle
  {
    protected final JsonFieldSchema fieldSchema;
    
    protected TupleFieldHandle( JsonFieldSchema schema ) {
      this.fieldSchema = schema;
    }
    
    @Override
    public JsonValue get( ) {
      return currentObject.get( fieldSchema.getName() );
    }

    @Override
    public ValueConversionError notSupportedError( String type ) {
      return new ValueConversionError( "Cannot convert " + fieldSchema.getType().toString() + " to " + type );
    }

    @Override
    public DataType getType() {
      return fieldSchema.getType();
    }

    @Override
    public Cardinality getCardinality() {
      return fieldSchema.getCardinality();
    }

  }
  
  private final BufferingTupleReader recordReader;
  JsonTupleSchema resultSchema;
  JsonTupleSchema inputSchema;
  private State state = State.START;
  private JsonObject currentObject;
  private FieldAccessor fieldAccessors[];
  
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
  public TupleSchema getSchema() {
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
  public TupleAccessor getTuple() {
    return this;
  }

  public void inferSchema() {
    JsonObject obj = recordReader.next();
    buildSchema( obj );
    recordReader.push( obj );
  }

  private FieldAccessor[] buildAccessors( ) {
    FieldAccessor accessors[] = new FieldAccessor[ resultSchema.getCount( ) ];
    for ( int i = 0;  i < accessors.length;  i++ ) {
      JsonFieldSchema field = (JsonFieldSchema) resultSchema.getField( i );
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
  public FieldAccessor getField(int i) {
    if ( i < 0  &&  i >= fieldAccessors.length ) {
      return null; }
    return fieldAccessors[ i ];
  }

  @Override
  public FieldAccessor getField(String name) {
    FieldSchema field = resultSchema.getField( name );
    if ( field == null ) {
      return null;
    }
    return fieldAccessors[ field.getIndex() ];
  }
  
  public boolean isEOF() {
    return state == State.EOF;
  }   
}

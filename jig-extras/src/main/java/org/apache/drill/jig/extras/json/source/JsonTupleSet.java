package org.apache.drill.jig.extras.json.source;

import javax.json.JsonObject;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.container.FieldValueContainerSet;
import org.apache.drill.jig.extras.json.reader.CapturingTupleReader;
import org.apache.drill.jig.extras.json.reader.JsonScannerException;
import org.apache.drill.jig.extras.json.reader.JsonTupleReader;
import org.apache.drill.jig.extras.json.reader.ReplayTupleReader;
import org.apache.drill.jig.extras.json.source.JsonAccessor.TupleObjectAccessor;
import org.apache.drill.jig.extras.json.source.ObjectParser.JsonObjectNode;
import org.apache.drill.jig.types.FieldValueFactory;

public class JsonTupleSet implements TupleSet
{
  private enum State { START, RUN, EOF, SCHEMA_CHANGE };
  
  public static class JsonTupleValue extends AbstractTupleValue {
    
    TupleSchema schema;
    private JsonObject currentObject;

    public JsonTupleValue(TupleSchema schema, FieldValueContainerSet containers) {
      super(containers);
      this.schema = schema;
    }
    
    public void bind( JsonObject tuple ) {
      currentObject = tuple;
    }

    @Override
    public TupleSchema schema() {
      return schema;
    }

    public Object getJsonObject() {
      return currentObject;
    }   
  }

  private JsonTupleReader recordReader;
  private State state = State.START;
  private JsonTupleValue tuple;
  FieldValueFactory factory = new FieldValueFactory( );
  private JsonObjectNode inputSchema;
  public int sampleSize = 3;
  
  public JsonTupleSet( JsonTupleReader recordReader ) {
    this.recordReader = recordReader;
  }
  
  @Override
  public TupleSchema schema() {
    return tuple.schema();
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
      tuple.bind( null );
      return false;
    }
    if ( obj.getValueType() != ValueType.OBJECT ) {
      throw new JsonScannerException( "Object is required but " + obj.getValueType() + " found." );
    }
//    if ( ! inputSchema.isCompatible( obj ) ) {
//      recordReader.push( obj );
//      state = State.SCHEMA_CHANGE;
//      return false;
//    }
    tuple.bind( obj );
    return true;
  }

  @Override
public TupleValue tuple() {
    return tuple;
  }

  public void inferSchema(boolean flatten) {
    ObjectParser parser = new ObjectParser( factory );
    @SuppressWarnings("resource")
    CapturingTupleReader captureReader = new CapturingTupleReader( recordReader );
    for ( int i = 0;  i < sampleSize;  i++ ) {
      JsonObject obj = captureReader.next();
      if ( obj == null ) {
        if ( i == 0 ) {
          state = State.EOF;
          return;
        }
        break;
      }
      parser.addObject( obj );
    }
    inputSchema = parser.getJsonSchema( );
    SchemaBuilder schemaBuilder = new SchemaBuilder( inputSchema );
    schemaBuilder.flatten( flatten );
    TupleSchema schema = schemaBuilder.build( );
    FieldValueContainerSet container = schemaBuilder.fieldValues( );
    TupleObjectAccessor tupleAccessor = schemaBuilder.rootAccessor( );
    tuple = new JsonTupleValue( schema, container );
    tupleAccessor.bind( tuple );
    
    recordReader = new ReplayTupleReader( captureReader.getTuples(), recordReader );
  }
  
  public boolean isEOF() {
    return state == State.EOF;
  }   
}

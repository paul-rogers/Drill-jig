package org.apache.drill.jig.extras.json;

import java.io.Reader;
import java.util.LinkedList;
import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSet;
import org.glassfish.json.CustomJsonReader;

public class JsonScanner implements AutoCloseable, ResultCollection
{
  protected interface JsonTupleReader extends AutoCloseable
  {
    JsonObject next();
    int getIndex( );
    @Override
    void close( );
  }
  
  protected static class BufferingTupleReader implements JsonTupleReader
  {
    JsonObject pushed;
    private JsonTupleReader input;
    
    public BufferingTupleReader( JsonTupleReader input ) {
      this.input = input;
    }
    
    @Override
    public JsonObject next()
    {
      if ( pushed != null ) {
        JsonObject obj = pushed;
        pushed = null;
        return obj;
      }
      return input.next( );
    }
    
    public void push(JsonObject obj) {
      pushed = obj;
    }

    @Override
    public void close() {
      input.close( );
    }

    @Override
    public int getIndex() {
      int index = input.getIndex( );
      if ( pushed != null ) {
        index--;
      }
      return index;
    }
  }
  
  private static class NullRecordReader implements JsonTupleReader
  {
    @Override
    public JsonObject next() {
      return null;
    }

    @Override
    public void close() {
    }

    @Override
    public int getIndex() {
      return 0;
    }
  }
  
  private static class JsonRecordReader implements JsonTupleReader
  {
    private JsonReader reader;
    int index = -1;

    public JsonRecordReader(JsonReader reader) {
      this.reader = reader;
    }

    @Override
    public JsonObject next() throws JsonScannerException {
      JsonStructure struct = reader.readObject();
      if ( struct == null ) {
        return null;
      }
      if ( struct.getValueType() != ValueType.OBJECT ) {
        throw new JsonScannerException( "Found unexpected JSON type " +
                    struct.getValueType( ).toString() +
                    " for tuple " + index );
      }
      index++;
      return (JsonObject) struct;
    }

    @Override
    public void close() {
      reader.close( );
    }

    @Override
    public int getIndex() {
      return index;
    }
  }
  
  public static class CapturingTupleReader implements JsonTupleReader
  {
    private JsonTupleReader reader;
    private List<JsonObject> tuples = new LinkedList<JsonObject>( );

    public CapturingTupleReader( JsonTupleReader reader ) {
      this.reader = reader;
    }
    
    @Override
    public void close() {
      reader.close( );
    }

    @Override
    public JsonObject next() {
      JsonObject obj = reader.next();
      if ( obj != null ) {
        tuples.add( obj );
      }
      return obj;
    }

    @Override
    public int getIndex() {
      return reader.getIndex();
    }
    
    public List<JsonObject> getTuples( ) {
      return tuples;
    }
    
  }
 
  public static class ReplayTupleReader implements JsonTupleReader
  {
    private JsonTupleReader reader;
    private List<JsonObject> tuples = new LinkedList<JsonObject>( );
    private int index = -1;
    
    public ReplayTupleReader( List<JsonObject> tuples, JsonTupleReader reader ) {
      this.tuples = tuples;
      this.reader = reader;
    }

    @Override
    public void close() {
      reader.close( );
    }

    @Override
    public JsonObject next() {
      if ( tuples.isEmpty() )
        return reader.next();
      index++;
      return tuples.remove( 0 );
    }

    @Override
    public int getIndex() {
      if ( tuples.isEmpty() )
        return reader.getIndex();
      return index;
    }
  }

  private static class JsonArrayReader implements JsonTupleReader
  {
    private final JsonArray jsonArray;
    private int index = -1;

    public JsonArrayReader(JsonArray array) {
      jsonArray = array;
    }

    @Override
    public JsonObject next() throws JsonScannerException {
      if ( index + 1 >= jsonArray.size() )
        return null;
      index++;
      JsonValue value = jsonArray.get( index );
      if ( value == null ) {
        throw new JsonScannerException( "Null value at index " + index +
            " of JSON input array." );
      }
      if ( value.getValueType() != ValueType.OBJECT ) {
        throw new JsonScannerException( "Found unexpected JSON type " +
                    value.getValueType( ).toString() +
                    " for tuple " + index );
      }
      return (JsonObject) value;
    }

    @Override
    public void close() { }

    @Override
    public int getIndex() { return index; }    
  }
  
  private final BufferingTupleReader recordReader;
  private int tupleSetIndex = -1;
  private boolean isEof;
  private JsonTupleSet tupleSet;
  
  @SuppressWarnings("resource")
  public JsonScanner( Reader in ) throws JsonScannerException {
    JsonReader reader = new CustomJsonReader( in );
    JsonStructure struct;
    struct = reader.read();
    if ( struct == null ) {
      recordReader = new BufferingTupleReader( new NullRecordReader( ) );
      reader.close( );
      isEof = true;
    }
    else if ( struct.getValueType() == ValueType.ARRAY ) {
      recordReader = new BufferingTupleReader( 
          new JsonArrayReader( (JsonArray) struct ) );
      reader.close( );
    }
    else if ( struct.getValueType() == ValueType.OBJECT ) {
      recordReader = new BufferingTupleReader( new JsonRecordReader( reader ) );
      recordReader.push( (JsonObject) struct );
    }
    else {
      throw new JsonScannerException( "Found unexpected JSON type " +
          struct.getValueType( ).toString() +
          " for first tuple" );
    }  
  }
  
  @Override
  public int getIndex() {
    return tupleSetIndex;
  }

  @Override
  public boolean next() {
    if ( isEof ) {
      return false;
    }
    if ( tupleSet == null ) {
      tupleSetIndex++;
      tupleSet = new JsonTupleSet( recordReader );
      tupleSet.inferSchema( );
    }
    else if ( tupleSet.isEOF() ) {
      tupleSet = null;
      isEof = true;
      return false;
    }
    else {
      tupleSetIndex++;
      JsonTupleSet oldSet = tupleSet;
      tupleSet = new JsonTupleSet( recordReader );
      tupleSet.evolveSchema( oldSet.inputSchema );
    }
    return true;
  }

  @Override
  public TupleSet getTuples() {
    return tupleSet;
  }

  @Override
  public void close() {
    recordReader.close( );
  }
}

package org.apache.drill.jig.extras.json;

import java.io.Reader;
import java.io.StringReader;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue.ValueType;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.extras.json.reader.BufferingTupleReader;
import org.apache.drill.jig.extras.json.reader.JsonArrayReader;
import org.apache.drill.jig.extras.json.reader.JsonRecordReader;
import org.apache.drill.jig.extras.json.reader.JsonScannerException;
import org.apache.drill.jig.extras.json.reader.NullRecordReader;
import org.glassfish.json.CustomJsonReader;

public class JsonResultCollection implements AutoCloseable, ResultCollection
{
  private final BufferingTupleReader recordReader;
  private int tupleSetIndex = -1;
  private boolean isEof;
  private JsonTupleSet tupleSet;
  
  @SuppressWarnings("resource")
  public JsonResultCollection( Reader in ) {
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
  
  public JsonResultCollection( String input ) {
    this( new StringReader( input ) );
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
//      JsonTupleSet oldSet = tupleSet;
      tupleSet = new JsonTupleSet( recordReader );
//      tupleSet.evolveSchema( oldSet.inputSchema );
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

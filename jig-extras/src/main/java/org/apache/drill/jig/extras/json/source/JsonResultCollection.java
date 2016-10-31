package org.apache.drill.jig.extras.json.source;

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
  private final boolean flatten;
  private int tupleSetIndex = -1;
  private boolean isEof;
  private JsonTupleSet tupleSet;
  
  public JsonResultCollection( Reader in ) {
    recordReader = prepareReader( in );
    flatten = false;
  }
  
  @SuppressWarnings("resource")
  private BufferingTupleReader prepareReader( Reader in ) {
    BufferingTupleReader objectReader;
    JsonReader reader = new CustomJsonReader( in );
    JsonStructure struct;
    struct = reader.read();
    if ( struct == null ) {
      objectReader = new BufferingTupleReader( new NullRecordReader( ) );
      reader.close( );
      isEof = true;
    }
    else if ( struct.getValueType() == ValueType.ARRAY ) {
      objectReader = new BufferingTupleReader( 
          new JsonArrayReader( (JsonArray) struct ) );
      reader.close( );
    }
    else if ( struct.getValueType() == ValueType.OBJECT ) {
      objectReader = new BufferingTupleReader( new JsonRecordReader( reader ) );
      objectReader.push( (JsonObject) struct );
    }
    else {
      throw new JsonScannerException( "Found unexpected JSON type " +
          struct.getValueType( ).toString() +
          " for first tuple" );
    }
    return objectReader;
  }
  
  public JsonResultCollection( String input ) {
    this( new StringReader( input ) );
  }
  
  public JsonResultCollection(
      JsonResultCollectionBuilder builder) {
    if ( builder.reader != null )
      recordReader = prepareReader( builder.reader );
    else if ( builder.input != null )
      recordReader = prepareReader( new StringReader( builder.input ) );
    else
      throw new IllegalArgumentException( "Reader or string required." );
    flatten = builder.flatten;
  }

  @Override
  public int index() {
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
      tupleSet.inferSchema( flatten );
    }
    else if ( tupleSet.isEOF() ) {
      tupleSet = null;
      isEof = true;
      return false;
    }
    else {
      tupleSetIndex++;
      tupleSet = new JsonTupleSet( recordReader );
    }
    return true;
  }

  @Override
  public TupleSet tuples() {
    return tupleSet;
  }

  @Override
  public void close() {
    recordReader.close( );
  }
}

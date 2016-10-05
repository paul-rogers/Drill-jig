package org.apache.drill.jig.extras.json.reader;

import java.io.Reader;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue.ValueType;

import org.glassfish.json.CustomJsonReader;

public class RecordReaderFactory {

  @SuppressWarnings("resource")
  public static BufferingTupleReader buildBufferingReader( Reader in ) throws JsonScannerException {
    JsonReader reader = new CustomJsonReader( in );
    JsonStructure struct;
    BufferingTupleReader recordReader;
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
    return reader;
  }
}

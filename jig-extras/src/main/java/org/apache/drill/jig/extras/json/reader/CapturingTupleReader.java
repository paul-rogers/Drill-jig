package org.apache.drill.jig.extras.json.reader;

import java.util.LinkedList;
import java.util.List;

import javax.json.JsonObject;

public class CapturingTupleReader implements JsonTupleReader
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
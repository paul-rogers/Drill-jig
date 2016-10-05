package org.apache.drill.jig.extras.json.reader;

import java.util.LinkedList;
import java.util.List;

import javax.json.JsonObject;

public class ReplayTupleReader implements JsonTupleReader
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
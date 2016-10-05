package org.apache.drill.jig.extras.json.reader;

import javax.json.JsonObject;

public class BufferingTupleReader implements JsonTupleReader
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
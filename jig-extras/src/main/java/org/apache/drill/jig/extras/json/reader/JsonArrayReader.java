package org.apache.drill.jig.extras.json.reader;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

public class JsonArrayReader implements JsonTupleReader
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
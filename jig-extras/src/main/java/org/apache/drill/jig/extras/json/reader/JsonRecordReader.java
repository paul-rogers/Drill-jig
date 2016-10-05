package org.apache.drill.jig.extras.json.reader;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue.ValueType;

public class JsonRecordReader implements JsonTupleReader
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
package org.apache.drill.jig.extras.json.reader;

import javax.json.JsonObject;

public class NullRecordReader implements JsonTupleReader
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
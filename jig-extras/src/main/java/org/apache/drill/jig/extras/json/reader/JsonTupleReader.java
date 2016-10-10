package org.apache.drill.jig.extras.json.reader;

import javax.json.JsonObject;

public interface JsonTupleReader extends AutoCloseable
{
  JsonObject next();
  int getIndex( );
  @Override
  void close( );
}
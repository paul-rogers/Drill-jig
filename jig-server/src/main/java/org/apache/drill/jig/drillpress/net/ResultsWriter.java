package org.apache.drill.jig.drillpress.net;

import org.apache.drill.jig.proto.SchemaResponse;

public interface ResultsWriter
{
  void writeEof( );
  void writeSchema( SchemaResponse schema );
  void noData( int statusCode, String msg );
  void writeRecord( int length, byte data[] );
}

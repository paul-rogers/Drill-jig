package org.apache.drill.jig.protocol;

import java.nio.ByteBuffer;

import org.apache.drill.jig.proto.InformationResponse;
import org.apache.drill.jig.proto.SchemaResponse;

public class DataResponse
{
  public enum Type {
    NO_DATA, SCHEMA, DATA, EOF };
  
  public DataResponse.Type type;
  public InformationResponse info;
  public SchemaResponse schema;
  public byte[] data;
  public ByteBuffer buf;
  
  public DataResponse( Type type ) {
    this.type = type;
  }

  public DataResponse(InformationResponse response) {
    this( Type.NO_DATA );
    info = response;
  }

  public DataResponse( SchemaResponse response ) {
    this( Type.SCHEMA );
    schema = response;
  }

  public DataResponse(byte[] response) {
    this( Type.DATA );
    data = response;
  }

  public DataResponse(ByteBuffer buf) {
    this( Type.DATA );
    this.buf = buf;
  }
}
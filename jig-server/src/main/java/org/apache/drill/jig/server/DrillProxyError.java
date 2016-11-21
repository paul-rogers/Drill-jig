package org.apache.drill.jig.server;

public enum DrillProxyError
{
  OK( 0, "00000", "OK" ),
  
  QUEUED( 100, "02000", "Query is queued" ),
  STARTING( 101, "02000", "Query is starting" ),
  NO_DATA( 102, "02000", "No data available" ),
  
  // Client errors
  
  ILLEGAL_MESSAGE_TYPE( 800, "DR800", "Invalid message type" ),
  PROTOBUF_DECODE_ERROR( 801, "DR801", "Invalid Protobuf" ),
  
  // Internal errors; problem with Drill itself.
  
  UNDEFINED_RESPONSE_TYPE( 900, "DR900", "Internal: Undefined response type" );
  
  private int code;
  private String sqlState;
  private String message;

  private DrillProxyError( int code, String sqlState, String message ) {
    this.code = code;
    this.sqlState = sqlState;
    this.message = message;
  }
  
  public int getCode( ) { return code; }
  public String getSqlState( ) { return sqlState; }
  public String getMessage( ) { return message; }
}

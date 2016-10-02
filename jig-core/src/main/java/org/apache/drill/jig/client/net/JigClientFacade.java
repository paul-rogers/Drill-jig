package org.apache.drill.jig.client.net;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.proto.ErrorResponse;
import org.apache.drill.jig.proto.ExecuteRequest;
import org.apache.drill.jig.proto.InformationResponse;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesRequest;
import org.apache.drill.jig.proto.LoginPropertiesResponse;
import org.apache.drill.jig.proto.LoginRequest;
import org.apache.drill.jig.proto.QueryRequest;
import org.apache.drill.jig.proto.SchemaResponse;
import org.apache.drill.jig.proto.SuccessResponse;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.protocol.HelloRequest;
import org.apache.drill.jig.protocol.HelloResponse;
import org.apache.drill.jig.protocol.MessageConstants;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;

/**
 * Implements the client side of the Jig API. This facade simply wraps
 * the message-passing details, it leaves the row, connection, results
 * and other abstractions to higher layers.
 */

public class JigClientFacade
{
  public enum State { START, PRE_LOGIN, READY, QUERY, END };
  
  private JigClientFacade.State state = State.START;
  private NetworkClient client;
  private ResponseParser responseParser;
  private LinkedBuffer buffer;
  
  public void connect( String host, int port ) throws JigException {
    if ( state != State.START ) {
      throw new IllegalStateException( "Already connected." );
    }
    
    client = new NetworkClient( host, port );
    try {
      client.connect();
    } catch (IOException e) {
      state = State.END;
      throw new JigIOException( "Connect failed", e );
    }
    
    responseParser = new ResponseParser( client );
    buffer = LinkedBuffer.allocate( 1024 );
    state = State.PRE_LOGIN;
  }
  
  public HelloResponse hello( HelloRequest request ) throws JigException {
    try {
      sendHeader( MessageConstants.HELLO_REQ, MessageConstants.HELLO_REQ_LEN );
      ByteBuffer buf = ByteBuffer.allocate( MessageConstants.HELLO_REQ_LEN );
      buf.putShort( (short) request.clientVersion );
      buf.putShort( (short) request.lowestClientVersion );
      client.write( buf );
      client.flush( );
    } catch (IOException e) {
      throw new JigIOException( "Connection lost", e );
    }
    int resp = receive( );
    if ( resp != MessageConstants.HELLO_RESP )
      throw unexpectedResponseError( MessageConstants.LIST_LOGINS_REQ, resp );
    return (HelloResponse) getResponse( );
  }
  
  public ListLoginsResponse getLoginMethods( ) throws JigException
  {
    assertPreLoginState( );      
    send( MessageConstants.LIST_LOGINS_REQ );
    int resp = receive( );
    if ( resp == MessageConstants.LIST_LOGIN_METHODS_RESP )
      return (ListLoginsResponse) getResponse( );
    throw unexpectedResponseError( MessageConstants.LIST_LOGINS_REQ, resp );
  }
  
  private void sendHeader( int requestType, int length ) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate( MessageConstants.REQ_HEADER_LEN );
    buf.putShort( (short) requestType );
    buf.putInt( length );
    client.write( buf );
  }
  
  private <T> void send(final int requestType, final T request, final Schema<T> schema) throws JigIOException {
    // Note: can skip this copy & write buffers directly.
    buffer.clear();
    byte msg[] = ProtobufIOUtil.toByteArray(request, schema, buffer);
    try {
      sendHeader( requestType, msg.length );
      client.write( msg, msg.length );
      client.flush( );
    } catch (IOException e) {
      close( );
      throw new JigIOException( "Connection lost", e );
    }
  }
    
  private void send(final int requestType) throws JigIOException {
    ByteBuffer buf = ByteBuffer.allocate( MessageConstants.REQ_HEADER_LEN );
    buf.putShort( (short) requestType );
    buf.putInt( 0 );
    try {
      client.write( buf );
    } catch (IOException e) {
      close( );
      throw new JigIOException( "Connection lost", e );
    }
  }
    
  private void assertPreLoginState( ) {
    if ( state == State.PRE_LOGIN )
      return;
    if ( state == State.START  ||  state == State.END ) {
      throw new IllegalStateException( "Not connected." );
    }
    throw new IllegalStateException( "Already logged in." );
  }
  
  public LoginPropertiesResponse getLoginProperties( String loginType ) throws JigException {
    assertPreLoginState( );      
    LoginPropertiesRequest req = new LoginPropertiesRequest( loginType );
    send( MessageConstants.LOGIN_PROPS_REQ, req, req.cachedSchema() );
    int resp = receive( );
    if ( resp == MessageConstants.LOGIN_PROPS_RESP )
      return (LoginPropertiesResponse) getResponse( );
    throw unexpectedResponseError( MessageConstants.LOGIN_PROPS_REQ, resp );
  }
  
  public void login( LoginRequest req ) throws JigException
  {   
    assertPreLoginState( );      
    send( MessageConstants.LOGIN_REQ, req, req.cachedSchema() );
    int resp = receive( );
    if ( resp == MessageConstants.OK_RESP ) {
      state = State.READY;
      return;
    }
    throw unexpectedResponseError( MessageConstants.LOGIN_REQ, resp );
  }
  
  public SuccessResponse executeStmt( ExecuteRequest req ) throws JigException {
    assertReadyState( );
    send( MessageConstants.EXEC_STMT_REQ, req, req.cachedSchema() );
    int resp = receive( );
    if ( resp == MessageConstants.SUCCESS_RESP )
      return (SuccessResponse) getResponse( );
    throw unexpectedResponseError( MessageConstants.EXEC_STMT_REQ, resp );
  }
  
  public void executeQuery( QueryRequest req ) throws JigException {
    assertReadyState( );
    send( MessageConstants.EXEC_QUERY_REQ, req, req.cachedSchema() );
    int resp = receive( );
    if ( resp == MessageConstants.OK_RESP ) {
      state = State.QUERY;
      return;
    }
    throw unexpectedResponseError( MessageConstants.LOGIN_REQ, resp );
  }
  
  private void assertReadyState() {
    if ( state == State.READY )
      return;
    if ( state == State.START  ||  state == State.END ) {
      throw new IllegalStateException( "Not connected." );
    }
    if ( state == State.PRE_LOGIN ) {
      throw new IllegalStateException( "Not logged in." );
    }
    if ( state == State.QUERY ) {
      throw new IllegalStateException( "Can't execute two queries at the same time." );
    }
    throw new IllegalStateException( "In unexpected state: " + state.name( ) );
  }
  
  public DataResponse getResults( ) throws JigException {
    assertQueryState( );
    send( MessageConstants.RESULTS_REQ );
    int respType = receive( );
    switch ( respType ) {
    case MessageConstants.EOF_RESP:
      state = State.READY;
      return new DataResponse( DataResponse.Type.EOF );
    case MessageConstants.INFO_RESP:
      return new DataResponse( (InformationResponse) getResponse( ) );
    case MessageConstants.SCHEMA_RESP:
      return new DataResponse( (SchemaResponse) getResponse( ) );
    case MessageConstants.RESULTS_RESP:
      return new DataResponse( ((ByteBuffer) getResponse( )).array( ) );
    }
    throw unexpectedResponseError( MessageConstants.RESULTS_REQ, respType );
  }
  
  private int receive( ) throws JigException {
    int respType;
    try {
      respType = responseParser.read();
    } catch (IOException e) {
      close( );
      throw new JigIOException( "Connection lost", e );
    }
    if ( respType == MessageConstants.ERROR_RESP ) {
      ErrorResponse error = (ErrorResponse) getResponse( );
      throw new JigServerException( error.getMessage(), error.getCode(), error.getSqlCode() );
    }
    return respType;
  }

  private Object getResponse() {
     return responseParser.getResponse();
  }

  private IllegalStateException unexpectedResponseError(int requestType, int respType) {
    return new IllegalStateException( "Unexpected response type " + respType +
        " for request type " + requestType );
  }

  private void assertQueryState() {
    if ( state == State.QUERY )
      return;
    if ( state == State.START  ||  state == State.END ) {
      throw new IllegalStateException( "Not connected." );
    }
    if ( state == State.PRE_LOGIN ) {
      throw new IllegalStateException( "Not logged in." );
    }
    throw new IllegalStateException( "No active query" );
  }
  
  public void cancelQuery( ) throws JigException {
    if ( state != State.QUERY )
      return;
    send( MessageConstants.CANCEL_QUERY_REQ );
    receive( );
  }
  
  public void close( ) {
    try {
      if ( state != State.START && state != State.END ) {
        send( MessageConstants.GOODBYE_REQ );
        receive( );
      }
      client.close( );
    } catch (Exception e) {
      // Ignore
    }
    client = null;
    state = State.END;
  }
}
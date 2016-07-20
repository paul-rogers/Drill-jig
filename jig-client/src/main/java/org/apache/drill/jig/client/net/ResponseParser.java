package org.apache.drill.jig.client.net;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.drill.jig.proto.ErrorResponse;
import org.apache.drill.jig.proto.InformationResponse;
import org.apache.drill.jig.proto.ListLoginsResponse;
import org.apache.drill.jig.proto.LoginPropertiesResponse;
import org.apache.drill.jig.proto.SchemaResponse;
import org.apache.drill.jig.proto.SuccessResponse;
import org.apache.drill.jig.protocol.HelloResponse;
import org.apache.drill.jig.protocol.MessageConstants;

import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;

public class ResponseParser
{
  public interface ResponseReader
  {
    Object read( ByteBuffer buf );
  }
  
  public static class HelloReader implements ResponseReader
  {
    @Override
    public Object read( ByteBuffer buf )
    {
      return new HelloResponse( buf.getShort(), buf.getShort() );
    }
  }
  
  public static class ResultsReader implements ResponseReader
  {
    @Override
    public Object read(ByteBuffer buf) {
      return buf;
    }   
  }
  
  public static class NullReader implements ResponseReader
  {
    @Override
    public Object read(ByteBuffer buf) {
      return null;
    }   
  }
  
  public static class ProtobufReader<T> implements ResponseReader
  {
    private Class<T> msgClass;
    private Schema<T> schema;

    public ProtobufReader( Class<T> msgClass, Schema<T> schema ) {
      this.msgClass = msgClass;
      this.schema = schema;
    }
    
    @Override
    public Object read(ByteBuffer buf) {
      try {
        T msg = msgClass.newInstance();
        ProtobufIOUtil.mergeFrom( buf.array(), msg, schema);
        return msg;
      } catch (InstantiationException e) {
        throw new IllegalStateException( e );
      } catch (IllegalAccessException e) {
        throw new IllegalStateException( e );
      }
    }
    
  }
  public static ResponseReader readers[] = makeReaders( );
  
  private NetworkClient client;
  private int responseType;
  private Object response;
  
  public ResponseParser( NetworkClient client ) {
    this.client = client;
  }

  private static ResponseReader[] makeReaders() {
    ResponseReader readers[] = new ResponseReader[ MessageConstants.RESP_COUNT ];
    
    readers[MessageConstants.HELLO_RESP] = new HelloReader( );
    readers[MessageConstants.OK_RESP] = new NullReader( );
    readers[MessageConstants.SUCCESS_RESP] =
        new ProtobufReader<SuccessResponse>( SuccessResponse.class, SuccessResponse.getSchema() );
    readers[MessageConstants.ERROR_RESP] =
        new ProtobufReader<ErrorResponse>( ErrorResponse.class, ErrorResponse.getSchema() );
    readers[MessageConstants.INFO_RESP] =
        new ProtobufReader<InformationResponse>( InformationResponse.class, InformationResponse.getSchema() );
    readers[MessageConstants.LIST_LOGIN_METHODS_RESP] =
        new ProtobufReader<ListLoginsResponse>( ListLoginsResponse.class, ListLoginsResponse.getSchema() );
    readers[MessageConstants.LOGIN_PROPS_RESP] =
        new ProtobufReader<LoginPropertiesResponse>( LoginPropertiesResponse.class, LoginPropertiesResponse.getSchema() );
    readers[MessageConstants.SCHEMA_RESP] =
        new ProtobufReader<SchemaResponse>( SchemaResponse.class, SchemaResponse.getSchema() );
    readers[MessageConstants.RESULTS_RESP] = new ResultsReader( );
    readers[MessageConstants.EOF_RESP] = new NullReader( );
    readers[MessageConstants.GOODBYE_RESP] = new NullReader( );
    return readers;
  }
        
  public int read( ) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate( MessageConstants.RESP_HEADER_LEN );
    client.read( buf, MessageConstants.RESP_HEADER_LEN );
    responseType = buf.getShort();
    int length = buf.getInt();
    if ( responseType < 0  ||  responseType >= readers.length || readers[responseType] == null ) {
      throw new IllegalStateException( "Unexpected server response type: " + responseType );
    }
    buf = ByteBuffer.allocate( length );
    client.read( buf, length );
    response = readers[responseType].read( buf );
    return responseType;
  }
  
  public Object getResponse( ) { return response; }
}
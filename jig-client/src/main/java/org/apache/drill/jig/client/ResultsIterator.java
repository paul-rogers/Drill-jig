package org.apache.drill.jig.client;

import java.nio.ByteBuffer;

import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.client.net.JigClientFacade;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.serde.TupleSetDeserializer;

/**
 * Iterates over results including multiple schema
 * changes.
 */

public class ResultsIterator
{
  /**
   * Iterates over the set of result messages for a query. 
   */
  
  public static class ResultIterator
  {
//    public enum ResponseType { SCHEMA, DATA, EOF };
    
//    private boolean isEof;
    private JigClientFacade client;
    private DataResponse response;
    
    public ResultIterator( JigClientFacade client ) {
      this.client = client;
    }
    
    public DataResponse.Type next( ) throws JigException {
      response = client.getResults();
      return response.type;
//      if ( response.type == DataResponse.Type.EOF ) {
//        return DataResponse.Type.EOF;
//      }
//      if ( response.type == DataResponse.Type.SCHEMA ) {
//        return ResponseType.SCHEMA;
//      }
//      if ( response.type == MessageConstants.ROWS_RESP ) {
//        return RESPONSE_TYPE.ROWS;
//      }
//      throw new IllegalStateException( "Unexpected server response type: " + responseType );
    }
    
    public DataResponse getResponse( ) {
      return response;
    }
  }
  
  /**
   * Iterates over the row within a single result
   * buffer.
   */
  
  public class ResultBlockIterator
  {
    ByteBuffer buffer;
    TupleSetDeserializer deserializer;
    int count;
    int posn;
    int index;
    int recLen;
    
    public void bind( DataResponse response ) {
      this.buffer = ByteBuffer.wrap( response.data );
      posn = 0;
      index = -1;
      recLen = 0;
      // TODO: set count
    }
    
    public boolean next( ) {
      return deserializer.deserializeTuple( buffer );
//      if ( index + 1 >= count )
//        return false;
//      posn += recLen;
//      // deserialize length
//      return true;
    }
    
    public int getIndex( ) {
      return index;
    }
    
    public int getOffset( ) {
      return posn;
    }
    
    public int getLength( ) {
      return recLen;
    }
    
//    public byte[] getBuffer( ) {
//      return buffer;
//    }
  }
}

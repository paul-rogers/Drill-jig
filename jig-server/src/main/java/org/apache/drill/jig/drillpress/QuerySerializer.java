package org.apache.drill.jig.drillpress;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.Statement;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.protocol.MessageConstants;
import org.apache.drill.jig.direct.DrillSession;
import org.apache.drill.jig.drillpress.net.RequestException;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.proto.ColumnSchema;
import org.apache.drill.jig.proto.QueryRequest;
import org.apache.drill.jig.proto.SchemaResponse;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.serde.TupleSetSerializer;

public class QuerySerializer
{
  public enum QueryState { START, SCHEMA, ROWS, END };
  
  private String sqlStmt;
  private QueryState state;
  private ResultCollection results;
  private TupleSetSerializer serializer;
  private TupleSet tupleSet;
  private int bufferSize;
  private ByteBuffer buf;
  private boolean rowPushed;

  public QuerySerializer(QueryRequest request) throws RequestException {
    sqlStmt = request.getStatement();
    bufferSize = request.getMaxResponseSizeK();
    if ( bufferSize < 1024 ) {
      throw new RequestException( "Buffer too small: " + bufferSize,
          MessageConstants.BUFFER_TOO_SMALL_ERROR );
    }
    buf = ByteBuffer.allocate( bufferSize );
  }

  public void start( DrillSession session ) throws RequestException {
    Statement statement = session.prepare( sqlStmt );
    try {
      results = statement.execute();
    } catch (JigException e) {
      throw new RequestException( "Execution failed: " + e.getMessage(),
          MessageConstants.EXECUTE_ERROR );
    }
    state = QueryState.START;
  }

  public DataResponse requestData() throws JigException {
    switch ( state ) {
    case ROWS:
      if ( serializeRows( ) )
        return new DataResponse( buf );
      
      // Fall through
      
    case SCHEMA:
    case START:
      if ( results.next() ) {
        tupleSet = results.getTuples();
        return schemaResponse( );
      }
      
      // Fall through
      
    case END:
      close( );
      return new DataResponse( DataResponse.Type.EOF );
    default:
      throw new IllegalStateException( "Unknown query state: " + state.name() );
    }
  }

  private DataResponse schemaResponse() throws RequestException {
    TupleSchema schema = tupleSet.schema();
    serializer = new TupleSetSerializer( schema );
    List<ColumnSchema> fields = new ArrayList<ColumnSchema>( );
    int count = schema.count();
    for ( int i = 0;  i < count;  i++ ) {
      FieldSchema field = schema.field( i );
      fields.add( new ColumnSchema( )
          .setName( field.name() )
          .setCardinality( field.getCardinality().cardinalityCode() )
          .setType( field.type().typeCode( ) ) );
    }
    state = QueryState.ROWS;
    return new DataResponse( new SchemaResponse( )
        .setColumnsList( fields ) );
  }

  private boolean serializeRows() throws JigException {
    buf.clear();
    int count = 0;
    for ( ; ; ) {
      if ( rowPushed ) {
        rowPushed = false;
      } else if ( ! tupleSet.next() ) {
        state = QueryState.SCHEMA;
        break;
      }
      TupleValue tuple = tupleSet.getTuple();
      if ( ! serializer.serializeTuple(buf, tuple) ) {
        if ( count == 0 ) {
          throw new RequestException( "Row too big for buffer",
              MessageConstants.ROW_TOO_BIG_ERROR );
        }
        break;
      }
      count++;
    }
    return count > 0;
  }

  public void close( ) throws JigException {
    serializer = null;
    tupleSet = null;
    if ( results != null ) {
      results.close( );
      results = null;
    }
    state = QueryState.END;
    buf = null;
  }
}

package org.apache.drill.jig.client;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;
import org.apache.drill.jig.client.net.JigClientFacade;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.proto.ColumnSchema;
import org.apache.drill.jig.proto.SchemaResponse;
import org.apache.drill.jig.protocol.DataResponse;
import org.apache.drill.jig.serde.deserializer.TupleSetDeserializer;

public class RemoteResultCollection implements ResultCollection
{
  private enum State { START, SCHEMA_CHANGE, END_OF_BUFFER, ROWS, EOF };
  
  private class RemoteTupleSet implements TupleSet
  {
    int index = -1;
    
    @Override
    public TupleSchema schema() {
      return deserializer.getSchema();
    }

    @Override
    public int getIndex() {
      return index;
    }

    @Override
    public boolean next() throws JigException {
      for ( ; ; ) {
        switch ( state ) {
        case SCHEMA_CHANGE:
        case EOF:
          return false;
        case END_OF_BUFFER:
          getResults( );
          break;
//          response = client.getResults();
//          switch ( response.type ) {
//          case DATA:
//            buffer = ByteBuffer.wrap( response.data );
//            state = State.ROWS;
//            break;
//          case EOF:
//            state = State.EOF;
//            return false;
//          case NO_DATA:
//            try {
//              Thread.sleep( conn.dataPollPeriodMs );
//            } catch (InterruptedException e) {
//              state = State.EOF;
//              return false;
//            }
//            break;
//          case SCHEMA:
//            state = State.SCHEMA_CHANGE;
//            return false;
//          default:
//            throw new IllegalArgumentException( "Unexpected get results type: " + response.type );
//          }
        case ROWS:
          if ( deserializer.deserializeTuple( buffer ) ) {
            index++;
            return true;
          }
          state = State.END_OF_BUFFER;
          break;
        default:
          throw new IllegalStateException( "Unexpected state: " + state );
        }
      }
    }

    @Override
    public TupleValue tuple() {
      return deserializer.getTuple();
    }

    public void reset() {
      index = -1;
    }
    
  }
  
  private RemoteConnection conn;
  private JigClientFacade client;
  private RemoteStatement stmt;
  private State state = State.START;
  private int tupleSetIndex = -1;
  private RemoteTupleSet tupleSet = new RemoteTupleSet( );
  private ByteBuffer buffer;
  private TupleSetDeserializer deserializer;
  private DataResponse response;

  public RemoteResultCollection(RemoteStatement stmt) {
    conn = stmt.getConnection( );
    this.stmt = stmt;
    client = conn.getClient( );
  }

  @Override
  public int index() {
    return tupleSetIndex;
  }

  @Override
  public boolean next() throws JigException {
    for ( ; ; ) {
      switch( state ) {
      case START:
        // No rows fetched yet. Will either be a schema or EOF
        
        getResults( );
        if ( state == State.ROWS ) {
          throw new IllegalStateException( "Received data packet before schema" );
        }
        break;
//        response = client.getResults();
//        switch ( response.type ) {
//        case DATA:
//          throw new IllegalStateException( "Received data packet before schema" );
//        case EOF:
//          state = State.EOF;
//          return false;
//        case NO_DATA:
//          try {
//            Thread.sleep( conn.dataPollPeriodMs );
//          } catch (InterruptedException e) {
//            state = State.EOF;
//            return false;
//          }
//          break;
//        case SCHEMA:
//          deserializer = new TupleSetDeserializer( );
//          deserializer.prepareSchema( translateSchema( response.schema ) );
//          tupleSetIndex++;
//          state = State.END_OF_BUFFER;
//          return true;
//        default:
//          throw new IllegalStateException( "Unexpected results response type:" + response.type );
//        }
      case END_OF_BUFFER:
        return true;
      case ROWS:
        // No schema change, wasn't really a reason to call this method.
        // We can just humor the caller and pretend that this is a new tuple set.
        tupleSet.reset( );
        return true;
      case SCHEMA_CHANGE:
        // The most recent call to fetch data got a new schema.
        deserializer = new TupleSetDeserializer( );
        deserializer.prepareSchema( translateSchema( response.schema ) );
        tupleSetIndex++;
        tupleSet.reset( );
        state = State.END_OF_BUFFER;
        return true;
      case EOF:
        return false;
      default:
        throw new IllegalStateException( "Unexpected state: " + state );    
      }
    }
  }
  
  private void getResults( ) throws JigException {
    for ( ; ; ) {
      response = client.getResults();
      switch ( response.type ) {
      case DATA:
        buffer = ByteBuffer.wrap( response.data );
        state = State.ROWS;
        return;
      case EOF:
        state = State.EOF;
        return;
      case NO_DATA:
        try {
          Thread.sleep( conn.dataPollPeriodMs );
        } catch (InterruptedException e) {
          state = State.EOF;
          return;
        }
        break;
      case SCHEMA:
        deserializer = new TupleSetDeserializer( );
        deserializer.prepareSchema( translateSchema( response.schema ) );
        tupleSetIndex++;
        state = State.END_OF_BUFFER;
        return;
      default:
        throw new IllegalStateException( "Unexpected results response type:" + response.type );
      }
    }
  }

  private TupleSchema translateSchema(SchemaResponse schema) {
    List<ColumnSchema> cols = schema.getColumnsList();
    if ( cols == null  ||  cols.size() == 0 )
      throw new IllegalStateException( "Empty schema" );
    TupleSchemaImpl tupleSchema = new TupleSchemaImpl( );
    for ( ColumnSchema col : cols )
    {
      tupleSchema.add( new FieldSchemaImpl( col.getName(),
          DataType.typeForCode( col.getType( ) ),
          col.getNullable( ) != 0 ) );
    }
    return tupleSchema;
  }

  @Override
  public TupleSet tuples() {
    switch ( state ) {
    case END_OF_BUFFER:
    case ROWS:
      return tupleSet;
    default:
      return null;
    }
  }

  @Override
  public void close() throws JigException {
    stmt.close();
  }

}

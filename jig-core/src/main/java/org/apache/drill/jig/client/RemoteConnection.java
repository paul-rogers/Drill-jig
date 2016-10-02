package org.apache.drill.jig.client;

import org.apache.drill.jig.api.Statement;
import org.apache.drill.jig.api.impl.AbstractDrillConnection;
import org.apache.drill.jig.client.net.JigClientFacade;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.proto.ExecuteRequest;
import org.apache.drill.jig.proto.QueryRequest;
import org.apache.drill.jig.proto.SuccessResponse;

public class RemoteConnection extends AbstractDrillConnection
{
  private JigClientFacade client;
  @SuppressWarnings("unused")
  private int sessionVersion;
  private RemoteStatement currentStatement;
  private int bufferSize;
  private int queryTimeoutSec;
  protected int dataPollPeriodMs;

  public RemoteConnection(ConnectionFactory factory) {
    this.client = factory.client;
    this.sessionVersion = factory.helloResponse.sessionVersion;
    this.bufferSize = factory.bufferSize;
    this.queryTimeoutSec = factory.timeout;
    dataPollPeriodMs = factory.dataPollPeriodMs;
  }

  @Override
  public int execute(String stmt) throws JigException {
    ExecuteRequest req = new ExecuteRequest( )
        .setStatement( stmt );
    SuccessResponse resp = client.executeStmt( req );
    return resp.getRowCount( );
  }

//  protected void checkError(Response response) throws JigServerException {
//    if ( response != null ) {
//      if ( response.payload == null )
//        throw new JigServerException( response.status );
//      else
//        throw new JigServerException(
//             new String( response.payload.array( ), Constants.utf8Charset ), 
//             response.status );
//    }
//  }

  @Override
  public Statement prepare(String stmt) {
    assertNotInStatement( );
    currentStatement = new RemoteStatement( this, stmt );
    return currentStatement;
  }

  private void assertNotInStatement() {
    if ( currentStatement != null )
      throw new IllegalStateException( "Jig supports only one active query per connection" );
  }

  protected void sendQuery( int msgType, String stmt ) throws JigException {
    QueryRequest req = new QueryRequest( )
        .setStatement( stmt )
        .setMaxResponseSizeK( bufferSize )
        .setMaxWaitSec( queryTimeoutSec );
    client.executeQuery( req );
  }

  @Override
  public void close() {
    client.close( );
  }

  public void closeStatement(RemoteStatement stmt) throws JigException {
    assert currentStatement == stmt;
    currentStatement = null;
    client.cancelQuery( );
  }

  public JigClientFacade getClient() {
    return client;
  }

}

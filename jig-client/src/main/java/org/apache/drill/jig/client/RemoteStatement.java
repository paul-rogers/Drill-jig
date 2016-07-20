package org.apache.drill.jig.client;

import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.Statement;
import org.apache.drill.jig.protocol.MessageConstants;

public class RemoteStatement implements Statement
{
  private RemoteConnection conn;
  private String stmt;

  public RemoteStatement(RemoteConnection conn, String stmt) {
    this.conn = conn;
    this.stmt = stmt;
  }

  @Override
  public ResultCollection execute() throws JigException {
    conn.sendQuery( MessageConstants.EXEC_QUERY_REQ, stmt );
    return new RemoteResultCollection( this );
  }
  
  @Override
  public void close() throws JigException {
    if ( conn != null ) {
      conn.closeStatement( this );
      conn = null;
    }
  }

  public RemoteConnection getConnection() {
    return conn;
  }

}

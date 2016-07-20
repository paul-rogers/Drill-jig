package org.apache.drill.jig.direct;

import java.util.List;
import java.util.Map;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;
import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.api.Statement;
import org.apache.drill.jig.api.impl.AbstractDrillConnection;

public class DrillSession extends AbstractDrillConnection
{
  private final DrillClient drillClient;
  
  protected DrillSession( DrillClient drillClient ) {
    this.drillClient = drillClient;      
  }
  
  public void setSessionOption( String key, String value ) {
    
  }
  
  public void setSessionOptions( Map<String,String> props ) {
    
  }
  
  @Override
  public int execute( String stmt ) throws JigException {
    try {
      List<QueryDataBatch> results = drillClient.runQuery(
          QueryType.SQL, stmt);
      int rowCount = results.size();
      for (QueryDataBatch queryDataBatch : results) {
        queryDataBatch.release();
      }
      return rowCount;
    } catch (RpcException e) {
      throw new DrillSessionException( "Execute failed: " + e.getMessage(), e );
    }
  }
  
//  @Override
//  public ResultCollection executeQuery( String stmt ) throws DrillException {
//  }
  
//  public QueryResults executeVectorQuery( String stmt ) throws DrillSessionException {
//    List<QueryDataBatch> results;
//    try {
//      results = drillClient.runQuery(
//          QueryType.SQL, stmt);
//    } catch (RpcException e) {
//      throw new DrillSessionException( "Execute failed", e );
//    }
//    return new QueryResults( results );
//  }
  
  public void startQuery( String stmt, UserResultsListener listener ) {
    drillClient.runQuery( QueryType.SQL, stmt, listener );
  }
  
  // Do something with async to avoid caching all results

  @Override
  public void close() {
    drillClient.close();
  }

  public BufferAllocator getRootAllocator() {
    return drillClient.getAllocator();
  }

  @Override
  public Statement prepare(String stmt) {
    return new DirectStatement( this, stmt );
  }

}
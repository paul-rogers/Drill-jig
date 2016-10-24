package org.apache.drill.jig.direct;

import java.util.concurrent.BlockingQueue;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.rpc.ConnectionThrottle;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.exec.rpc.user.UserResultsListener;

import com.google.common.collect.Queues;

/**
 * Drill query event listener that buffers rows into a producer-consumer
 * queue. Allows rows to be received asynchronously, but processed by
 * a synchronous reader.
 */

public class BufferingQueryEventListener implements UserResultsListener
{
  public static class QueryEvent
  {
    public enum Type { QUERY_ID, BATCH, EOF, ERROR }
    
    final Type type;
    QueryId queryId;
    QueryDataBatch batch;
    UserException error;
    
    public QueryEvent(QueryId queryId) {
      this.queryId = queryId;
      this.type = Type.QUERY_ID;
    }
    
    public QueryEvent(UserException ex) {
      error = ex;
      type = Type.ERROR;
    }
    
    public QueryEvent(QueryDataBatch batch) {
      this.batch = batch;
      type = Type.BATCH;
    }
    
    public QueryEvent(Type type) {
      this.type = type;
    }
  }
  
  private BlockingQueue<QueryEvent> queue = Queues.newLinkedBlockingQueue();

  @Override
  public void queryIdArrived(QueryId queryId) {
    silentPut( new QueryEvent( queryId ) );
  }

  @Override
  public void submissionFailed(UserException ex) {
    silentPut( new QueryEvent( ex ) );
  }

  @Override
  public void dataArrived(QueryDataBatch result, ConnectionThrottle throttle) {
    silentPut( new QueryEvent( result ) );
  }

  @Override
  public void queryCompleted(QueryState state) {
    silentPut( new QueryEvent( QueryEvent.Type.EOF ) );
  }
  
  private void silentPut( QueryEvent event ) {
    try {
      queue.put( event );
    } catch (InterruptedException e) {
      // What to do, what to do...
      e.printStackTrace();
    }
  }
  
  public QueryEvent get( ) {
    try {
      return queue.take( );
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return null;
    }
  }
}
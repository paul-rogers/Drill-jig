package org.apache.drill.jig.direct;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.apache.drill.jig.direct.BufferingQueryEventListener.QueryEvent;

/**
 * API to iterate over incoming records. This reader reverses the normal
 * Drill client flow in which the Drill client pushes records to the
 * consumer. This class, in conjunction with the
 * {@class BufferingQueryEventListener} class, implements a classic
 * producer-consumer mechanism:
 * <pre>
 * server &rarr; listener &rarr; queue
 * caller &larr; VectorRecordReader &larr; queue
 * </pre>
 * Vectors are wrapped to provide a more record-like form consumable
 * by the client API classes.
 */

public class VectorRecordReader implements AutoCloseable
{
  public enum Event { SCHEMA, BATCH, RECORD, EOF };
  
  DrillSession session;
  BufferingQueryEventListener input;
  public RecordBatchLoader loader;
  private QueryId queryId;
  private boolean isEof;
  private boolean schemaChanged;
  private int batchCount;
  private int recordCount;
  private QueryDataBatch currentBatch;
  private  VectorRecordIterator recordIter;
  
  public VectorRecordReader( DrillSession session, String stmt ) {
    input = new BufferingQueryEventListener( );
    loader = new RecordBatchLoader( session.getRootAllocator( ) );
    session.startQuery( stmt, input );
  }
  
  public Event next( ) {
    Event okEvent = Event.RECORD;
    for ( ; ; ) {
      if ( isEof ) {
        return Event.EOF;
      }
      if ( recordIter != null  &&  recordIter.next() != null ) {
        recordCount++;
        return okEvent;
      }
      if ( ! readBatch( ) )
        return Event.EOF;
      if ( schemaChanged )
        return Event.SCHEMA;
      else
        okEvent = Event.BATCH;
    }
  }
  
  private boolean readBatch( ) {
    if ( currentBatch != null ) {
      currentBatch.release();
      currentBatch = null;
    }
    for ( ; ; ) {
      QueryEvent event = input.get();
      switch ( event.type ) {
      case BATCH:
        try {
          batchCount++;
          currentBatch = event.batch;
          schemaChanged = loader.load( currentBatch.getHeader().getDef(), currentBatch.getData() );
          recordIter = new VectorRecordIterator( loader );
        } catch (SchemaChangeException e) {
          // According to code, this no longer happens.
        }
        return true;
      case EOF:
        isEof = true;
        return false;
      case ERROR:
        throw event.error;
      case QUERY_ID:
        queryId = event.queryId;
        break;
      default:
        assert false;
        break;
      }
    }
  }
  
  public BatchSchema getSchema( ) {
    return loader.getSchema();
  }
  
  public VectorRecord getRecord( ) { return recordIter; }  
  public QueryId getQueryId( ) { return queryId; }
  public int getRecordIndex( ) { return recordCount - 1; }
  public int getBatchIndex( ) { return batchCount - 1; }
  public boolean isEof( ) { return isEof; }
  public int getBatchCount( ) { return batchCount; }
  public int getRecordCount( ) { return recordCount; }
  public int getBatchRecordIndex( ) {
    if ( recordIter == null ) {
      return -1;
    }
    return recordIter.getIndex( );
  }
  
  @Override
  public void close( ) {
    loader.clear();
    if ( currentBatch != null ) {
      currentBatch.release();
      currentBatch = null;
    }
    if ( isEof ) {
      return;
    }
    outer:
    for ( ; ; ) {
      QueryEvent event = input.get();
      switch ( event.type ) {
      case BATCH:
        event.batch.release();
        break;
      case EOF:
        break outer;
      default:
        break;
      }
    }
  }
}
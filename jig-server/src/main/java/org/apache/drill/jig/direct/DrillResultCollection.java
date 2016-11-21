package org.apache.drill.jig.direct;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;

/**
 * Maps a Drill query into the Jig result set concept. Each query corresponds
 * to a result set. Each schema change corresponds to a Tuple set. Each
 * record corresponds to a Tuple. Each value vector corresponds to a field.
 */

public class DrillResultCollection implements ResultCollection
{
  public class DrillTupleSet implements TupleSet
  {
    private DrillRootTupleValue tuple;

    public DrillTupleSet( ) {
    }
    
    protected void buildSchema( ) {
      tuple = new TupleBuilder( reader.getSchema() ).build( );
      tuple.bindReader( reader );
      tuple.bindVectors();
    }

    @Override
    public TupleSchema schema() {
      return tuple.schema( );
    }

    @Override
    public int getIndex() {
      return reader.getBatchRecordIndex( );
    }

    @Override
    public boolean next() {
      if ( state == State.EOF )
        return false;
      switch ( reader.next() ) {
      case EOF:
        state = State.EOF;
        tuple.reset( );
        return false;
      case BATCH:
        tuple.bindVectors( );        
        // Fall through
      case RECORD:
        state = State.RECORDS;
        tuple.start( );
        return true;
      case SCHEMA:
        state = State.SCHEMA_CHANGE;
        return false;
      default:
        assert false;
        return false;
      }
    }

    @Override
    public TupleValue tuple() {
      return tuple;
    }
  }
  
  private enum State { START, ROWS, SCHEMA_CHANGE, RECORDS, EOF };
  
  protected VectorRecordReader reader;
  protected State state = State.START;
  protected DrillTupleSet tupleSet = new DrillTupleSet( );

  public DrillResultCollection(VectorRecordReader reader) {
    this.reader = reader;
  }

  @Override
  public int index() {
    return reader.getBatchIndex();
  }

  @Override
  public boolean next() {
    if ( state == State.EOF ) {
      return false;
    }
    if ( state == State.START ) {
      switch ( reader.next() ) {
      case EOF:
        state = State.EOF;
        return false;
      case SCHEMA:
        state = State.SCHEMA_CHANGE;
        break;
      default:
        assert false;
        return false;
      }
    }
    if ( state == State.SCHEMA_CHANGE ) {
      tupleSet.buildSchema();
      return true;
    }
    if ( state == State.RECORDS ) {
      throw new IllegalStateException( "Move to the next tuple set only when the previous is complete." );
    }
    
    // If the caller calls this out of sequence (before a schema change),
    // just silently pretend that a schema change occurred.
    
    return true;   
  }

  @Override
  public TupleSet tuples() {
    if ( state == State.SCHEMA_CHANGE  ||  state == State.ROWS ) {
      return tupleSet;
    }
    return null;
  }
  
  @Override
  public void close( ) {
    if ( reader != null )
      reader.close();
    reader = null;
  }
}

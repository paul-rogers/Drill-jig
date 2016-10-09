package org.apache.drill.jig.direct;

import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;

public class DrillResultCollection implements ResultCollection
{
  public class DrillTupleSet implements TupleSet
  {
    private DrillTupleSchema schema;
    private DrillTupleAccessor tuple;

    public DrillTupleSet( ) {
    }
    
    protected void buildSchema( ) {
      schema = new DrillTupleSchemaBuilder( reader.getSchema() ).build( );
      tuple = new DrillTupleAccessor( reader, schema );
    }

    @Override
    public TupleSchema schema() {
      return schema;
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
        return false;
      case RECORD:
        state = State.RECORDS;
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

    public void reset() {
      tuple.bindVectors();
    }
  }
  
  public static class DrillTupleAccessor implements TupleValue
  {
    private DrillTupleSchema schema;

    public DrillTupleAccessor( VectorRecordReader reader, DrillTupleSchema schema ) {
      this.schema = schema;
      
      int n = schema.accessors.length;
      for ( int i = 0;  i < n;  i++ ) {
        schema.accessors[i].bind( reader, schema.field( i ) );
      }
    }
    
    public void bindVectors( ) {
      int n = schema.accessors.length;
      for ( int i = 0;  i < n;  i++ ) {
        schema.accessors[i].bindVector( );
      }
    }
    
    @Override
    public TupleSchema schema() {
      return schema;
    }

    @Override
    public FieldValue field(int i) {
      if ( i < 0  &&  i >= schema.count() )
        return null;
      return schema.accessors[i];
    }

    @Override
    public FieldValue field(String name) {
      FieldSchema field = schema.field(name);
      if ( field == null )
        return null;
      else
        return schema.accessors[ field.index() ];
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
  public int getIndex() {
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
  public TupleSet getTuples() {
    if ( state == State.SCHEMA_CHANGE  ||  state == State.ROWS ) {
      tupleSet.reset( );
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

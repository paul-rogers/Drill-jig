package org.apache.drill.jig.api.array;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldAccessor;
import org.apache.drill.jig.api.FieldAccessor.ScalarAccessor;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleAccessor;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.impl.FieldSchemaImpl;
import org.apache.drill.jig.api.impl.TupleSchemaImpl;

public class ArrayResultCollection implements ResultCollection {

  public static class Batch
  {
    String names[];
    Object data[][];
    
    public Batch( String names[], Object data[][] ) {
      this.names = names;
      this.data = data;
    }
  }
  
  public class ArrayFieldAccessor implements FieldAccessor, ScalarAccessor
  {
    private int fieldIndex;
    
    public ArrayFieldAccessor(int fieldIndex) {
      this.fieldIndex = fieldIndex;
    }

    @Override
    public DataType getType() {
      return tupleSet.getSchema().getField( fieldIndex ).getType();
    }

    @Override
    public Cardinality getCardinality() {
      return tupleSet.getSchema().getField( fieldIndex ).getCardinality();
    }

    @Override
    public boolean isNull() {
      return getValue( ) == null;
    }

    @Override
    public ScalarAccessor asScalar() {
      return this;
    }

    @Override
    public ArrayAccessor asArray() {
      return null;
    }

    @Override
    public AnyAccessor asAny() {
      return null;
    }

    @Override
    public boolean getBoolean() {
      return (Boolean) getValue( );
    }

    @Override
    public byte getByte() {
      return (Byte) getValue( );
    }

    @Override
    public short getShort() {
      return (Short) getValue( );
    }

    @Override
    public int getInt() {
      return (Integer) getValue( );
    }

    @Override
    public long getLong() {
      return (Long) getValue( );
    }

    @Override
    public float getFloat() {
      return (Float) getValue( );
    }

    @Override
    public double getDouble() {
      return (Double) getValue( );
    }

    @Override
    public BigDecimal getDecimal() {
      return (BigDecimal) getValue( );
    }

    @Override
    public String getString() {
      return (String) getValue( );
    }

    @Override
    public byte[] getBlob() {
      return null;
    }

    @Override
    public LocalDate getDate() {
      return (LocalDate) getValue( );
    }

    @Override
    public LocalDateTime getDateTime() {
      return (LocalDateTime) getValue( );
    }

    @Override
    public Period getUTCTime() {
      return (Period) getValue( );
    }

    @Override
    public Object getValue() {
      return tupleSet.batch.data[tupleSet.rowIndex][fieldIndex];
    }
    
  }
  
  public class ArrayTupleAccessor implements TupleAccessor {

    ArrayFieldAccessor fields[];
    
    @Override
    public TupleSchema getSchema() {
      return tupleSet.getSchema();
    }

    @Override
    public FieldAccessor getField(int i) {
      if ( i < 0  ||  i >= fields.length )
        return null;
      return fields[i];
    }

    @Override
    public FieldAccessor getField(String name) {
      FieldSchema field = getSchema( ).getField( name );
      if ( field == null )
        return null;
      return getField( field.getIndex() );
    }    
  }
  
  public class ArrayTupleSet implements TupleSet {

    Batch batch;
    TupleSchemaImpl schema = new TupleSchemaImpl( );
    int rowIndex = -1;
    ArrayTupleAccessor tuple;
    
    protected void start( ) {
      batch = batches[index];
      tuple = new ArrayTupleAccessor( );
      tuple.fields = new ArrayFieldAccessor[ batch.names.length ];
      batch = batches[index];
      for ( int i = 0;  i < batch.names.length;  i++ ) {
        DataType type = DataType.NULL;
        if ( batch.data != null  &&  batch.data.length > 0 ) {
          Object value = batch.data[0][i];
          if ( value == null )
            type = DataType.ANY;
          else if ( value instanceof String )
            type = DataType.STRING;
          else if ( value instanceof Boolean)
            type = DataType.BOOLEAN;
          else if ( value instanceof Integer )
            type = DataType.INT32;
          else if ( value instanceof Short )
            type = DataType.INT16;
          else if ( value instanceof Long )
            type = DataType.INT64;
          else if ( value instanceof Byte )
            type = DataType.INT8;
          else if ( value instanceof BigDecimal )
            type = DataType.DECIMAL;
          else if ( value instanceof Float )
            type = DataType.FLOAT32;
          else if ( value instanceof Double )
            type = DataType.FLOAT64;
          else
            throw new IllegalArgumentException( "Column " + i + " of type " + value.getClass().getSimpleName() );
        }
        schema.add( new FieldSchemaImpl( batch.names[i], type, Cardinality.OPTIONAL ) );
        tuple.fields[i] = new ArrayFieldAccessor( i );
      }
    }

    @Override
    public TupleSchema getSchema() {
      return schema;
    }

    @Override
    public int getIndex() {
      return rowIndex;
    }

    @Override
    public boolean next() throws JigException {
      if ( rowIndex + 1 < batch.data.length ) {
        rowIndex++;
        return true;
      }
      return false;
    }

    @Override
    public TupleAccessor getTuple() {
      return tuple;
    } 
  }

  private Batch[] batches;
  private int index = -1;
  private ArrayTupleSet tupleSet;
  
  public ArrayResultCollection( Batch batches[] ) {
    this.batches = batches;
    tupleSet = new ArrayTupleSet( );
  }
  
  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public boolean next() throws JigException {
    if ( index + 1 < batches.length ) {
      index++;
      tupleSet.start();
      return true;
    }
    return false;
  }

  @Override
  public TupleSet getTuples() {
    if ( index < batches.length )
      return tupleSet;
    return null;
  }

  @Override
  public void close() {
  }
}

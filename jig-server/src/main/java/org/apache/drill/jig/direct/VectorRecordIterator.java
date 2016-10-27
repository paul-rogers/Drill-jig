package org.apache.drill.jig.direct;

import java.util.Iterator;

import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;

public class VectorRecordIterator implements Iterator<VectorRecord>, VectorRecord
{
  private int rowCount;
  private int rowIndex = -1;
  private VectorWrapper<?>[] vectors;
  
  public VectorRecordIterator( VectorAccessible va ) {
    rowCount = va.getRecordCount();
    int fieldCount = va.getSchema().getFieldCount();
    vectors = new VectorWrapper<?>[ fieldCount ];
    int i = 0;
    for ( VectorWrapper<?> vw : va ) {
      vectors[i++] = vw;
    }
  }
  
  @Override
  public boolean hasNext() {
    return rowIndex + 1 < rowCount;
  }

  @Override
  public VectorRecord next() {
    if ( rowIndex < rowCount ) {
      rowIndex++;
    }
    if ( rowIndex == rowCount ) {
      return null;
    }
    return this;
  }
  
  
  @Override
  public Object getValue( int fieldIndex ) {
    return vectors[ fieldIndex ].getValueVector().getAccessor().getObject( rowIndex );
  }
  
  @Override
  public VectorWrapper<?> getVector( int columnIndex ) {
    return vectors[ columnIndex ];
  }

  @Override
  public int getFieldCount() {
    return vectors.length;
  }

  public int getIndex() {
    return rowIndex;
  }
}
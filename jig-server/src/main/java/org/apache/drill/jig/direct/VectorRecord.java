package org.apache.drill.jig.direct;

import org.apache.drill.exec.record.VectorWrapper;

public interface VectorRecord
{
  int getFieldCount( );
  Object getValue( int fieldIndex );
  VectorWrapper<?> getVector( int columnIndex );
}
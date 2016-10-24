package org.apache.drill.jig.direct;

import org.apache.drill.exec.record.VectorWrapper;

public interface VectorRecord
{
  int getFieldCount( );
  VectorWrapper<?> getVector( int columnIndex );
}
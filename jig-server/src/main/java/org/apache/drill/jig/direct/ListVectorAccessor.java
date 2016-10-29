package org.apache.drill.jig.direct;

import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;

public class ListVectorAccessor extends VectorAccessor
    implements ObjectAccessor {

  ListVector.Accessor accessor;
  
  @Override
  public void bindVector( ) {
    super.bindVector( );
    accessor = ((ListVector) getVector()).getAccessor( );
  }
  
  @Override
  public Object getObject() {
    return accessor.getObject( rowIndex( ) );
  }

}

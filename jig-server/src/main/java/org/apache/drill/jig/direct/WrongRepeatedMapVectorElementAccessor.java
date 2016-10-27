package org.apache.drill.jig.direct;

import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector.RepeatedMapAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.direct.VectorAccessor.DrillElementAccessor;

public class WrongRepeatedMapVectorElementAccessor extends DrillElementAccessor implements ObjectAccessor {

  RepeatedMapAccessor accessor;
  
  @Override
  public void bindVector( ) {
    super.bindVector( );
    accessor = ((RepeatedMapVector) getVector()).getAccessor( );
  }

  @Override
  public Object getObject() {
    return accessor.getObject( elementIndex );
  }

}
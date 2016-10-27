package org.apache.drill.jig.direct;

import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector.RepeatedMapAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.direct.VectorAccessor.DrillElementAccessor;

public class MapVectorAccessor extends VectorAccessor
    implements ObjectAccessor {

  MapVector.Accessor accessor;
  
  @Override
  public void bindVector( ) {
    super.bindVector( );
    accessor = ((MapVector) getVector()).getAccessor( );
  }

  @Override
  public Object getObject() {
    return accessor.getObject( rowIndex( ) );
  }

  public static class RepeatedMapVectorAccessor extends VectorAccessor implements ObjectAccessor {

    RepeatedMapAccessor accessor;
    
    @Override
    public void bindVector( ) {
      super.bindVector( );
      accessor = ((RepeatedMapVector) getVector()).getAccessor( );
    }

    @Override
    public Object getObject() {
      return accessor.getObject( rowIndex( ) );
    }

  }
}

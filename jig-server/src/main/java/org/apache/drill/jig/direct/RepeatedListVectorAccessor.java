package org.apache.drill.jig.direct;

import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;

public class RepeatedListVectorAccessor extends VectorAccessor implements ObjectAccessor {

  private BaseRepeatedValueVector.BaseRepeatedAccessor accessor;

  @Override
  public void bindVector( ) {
    super.bindVector();
    accessor = ((RepeatedListVector) getVector( )).getAccessor();
  }

  /**
   * Get the value at the current row index. The value is a Java
   * List of the underlying type.
   */
  
  @Override
  public Object getObject() {
    return accessor.getObject( rowIndex( ) );
  } 
}

package org.apache.drill.jig.direct;

import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector.RepeatedAccessor;
import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ArrayAccessor;

public class DrillRepeatedVectorAccessor extends VectorAccessor
    implements ArrayAccessor {

  private final DrillElementAccessor memberAccessor;
  private RepeatedAccessor accessor;
  
  public DrillRepeatedVectorAccessor( DrillElementAccessor memberAccessor ) {
    this.memberAccessor = memberAccessor;
  }
  
  @Override
  public void bindVector( ) {
    super.bindVector();
    accessor = ((BaseRepeatedValueVector) getVector( )).getAccessor();
  }
  
  @Override
  public FieldAccessor memberAccessor() {
    return memberAccessor;
  }

  @Override
  public int size() {
    return accessor.getInnerValueCountAt( rowIndex( ) );
  }

  @Override
  public Object getValue() {
    return null;
  }

  @Override
  public void select(int index) {
    memberAccessor.bind( index );
  }
}

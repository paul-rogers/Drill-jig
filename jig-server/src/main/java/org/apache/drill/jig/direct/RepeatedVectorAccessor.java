package org.apache.drill.jig.direct;

import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector.RepeatedAccessor;
import org.apache.drill.jig.accessor.FieldAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ArrayAccessor;

/**
 * Accessor for a the Jig array implied by a Drill repeated field. In
 * Drill, the array is virtual: all that actually exists are the field
 * values themselves. This accessor creates the array as a "useful
 * fiction" on top of the Drill implementation.
 */

public class RepeatedVectorAccessor extends VectorAccessor
    implements ArrayAccessor {

  private final DrillElementAccessor memberAccessor;
  private RepeatedAccessor accessor;
  
  public RepeatedVectorAccessor( DrillElementAccessor memberAccessor ) {
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
  public void select(int index) {
    memberAccessor.bind( index );
  }
}

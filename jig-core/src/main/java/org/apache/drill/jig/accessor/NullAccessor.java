package org.apache.drill.jig.accessor;

import org.apache.drill.jig.util.JigUtilities;

/**
 * Trivial accessor used for fields of type Null or Undefined.
 */

public class NullAccessor implements FieldAccessor {

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    buf.append( toString( ) );
   }

  @Override
  public String toString( ) {
    StringBuilder buf = new StringBuilder( );
    JigUtilities.objectHeader(buf, this);
    buf.append( "]" );
    return buf.toString();
  }
}

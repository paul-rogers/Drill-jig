package org.apache.drill.jig.accessor;

import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.util.JigUtilities;

public class CachedObjectAccessor implements ObjectAccessor {

  private Object cachedValue;

  public void bind( Object value ) {
    cachedValue = value;
  }

  @Override
  public boolean isNull() {
    return cachedValue == null;
  }

  @Override
  public Object getObject() {
    return cachedValue;
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "]" );
  }
}

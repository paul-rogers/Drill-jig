package org.apache.drill.jig.accessor;

import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.Resetable;
import org.apache.drill.jig.util.JigUtilities;

/**
 * Object accessor that caches the retrieved object for the duration of
 * a tuple.
 */

public class ReadOnceObjectAccessor implements ObjectAccessor, Resetable {

  private final ObjectAccessor sourceAccessor;
  private boolean isCached;
  private Object cachedValue;

  public ReadOnceObjectAccessor( ObjectAccessor sourceAccessor ) {
    this.sourceAccessor = sourceAccessor;
  }

  @Override
  public boolean isNull() {
    return getObject( ) == null;
  }

  @Override
  public void reset() {
    isCached = false;
  }

  @Override
  public Object getObject() {
    if ( ! isCached ) {
      cachedValue = sourceAccessor.getObject();
      isCached = true;
    }
    return cachedValue;
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( " source = " );
    sourceAccessor.visualize( buf, indent + 1 );
    buf.append( "]" );
  }
}

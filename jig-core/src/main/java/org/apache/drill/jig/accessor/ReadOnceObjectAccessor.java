package org.apache.drill.jig.accessor;

import org.apache.drill.jig.accessor.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.Resetable;

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
}

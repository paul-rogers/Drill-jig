package org.apache.drill.jig.types;

import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;

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

}

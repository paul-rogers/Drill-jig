package org.apache.drill.jig.types;

public class NullAccessor implements FieldAccessor {

  @Override
  public boolean isNull() {
    return true;
  }

}

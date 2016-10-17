package org.apache.drill.jig.accessor;

/**
 * Trivial accessor used for fields of type Null or Undefined.
 */

public class NullAccessor implements FieldAccessor {

  @Override
  public boolean isNull() {
    return true;
  }

}

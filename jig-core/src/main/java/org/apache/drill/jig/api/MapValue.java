package org.apache.drill.jig.api;

import java.util.Collection;

public interface MapValue {
  int size();

  Collection<String> keys();

  FieldValue get(String key);
}

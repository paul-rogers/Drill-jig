package org.apache.drill.jig.types;

import org.apache.drill.jig.api.DataType;

/**
 * Provides a cache of reusable field values. Used to implement Variant
 * fields.
 */

public class FieldValueCache {

  private final AbstractFieldValue values[] = new AbstractFieldValue[ DataType.values().length ];
  private final FieldValueFactory factory;
  
  public FieldValueCache(FieldValueFactory factory) {
    this.factory = factory;
  }
  
  public AbstractFieldValue get( DataType type ) {
    int index = type.ordinal();
    if ( values[ index ] == null )
      values[index] = factory.buildValue( type );
    return values[index];
  }
  
}

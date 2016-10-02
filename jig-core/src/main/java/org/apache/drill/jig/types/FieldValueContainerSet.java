package org.apache.drill.jig.types;

import org.apache.drill.jig.api.FieldValue;

public class FieldValueContainerSet {

  private final FieldValueContainer containers[];
  
  public FieldValueContainerSet( FieldValueContainer containers[] ) {
    this.containers = containers;
  }
  
  public FieldValue field( int index ) {
    if ( index < 0  ||  containers.length <= index )
      return null;
    return containers[index].get();
  }
}

package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.types.FieldAccessor.Resetable;
import org.apache.drill.jig.types.FieldValueContainerSet;

public abstract class AbstractTupleValue implements TupleValue {

  protected final FieldValueContainerSet containers;
  public Resetable resetable[];
  private final int size;
  
  public AbstractTupleValue( FieldValueContainerSet containers ) {
    this.containers = containers;
    size = containers.size( );
  }
  
  public void reset( ) {
    if ( resetable == null )
      return;
    for ( int i = 0; i < resetable.length;  i++ )
      resetable[i].reset( );
  }

  @Override
  public FieldValue field(int i) {
    if ( i < 0  ||  size <= i )
      return null;
    return fieldValue(i);
  }
  
  protected FieldValue fieldValue(int i ) {
    return containers.field(i);
  }

  @Override
  public FieldValue field(String name) {
    FieldSchema field = schema().field(name);
    if (field == null)
      return null;
    return fieldValue(field.index());
  }

}

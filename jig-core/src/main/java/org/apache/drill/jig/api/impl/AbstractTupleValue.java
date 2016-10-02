package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.types.FieldAccessor.Resetable;
import org.apache.drill.jig.types.FieldValueContainerSet;

public abstract class AbstractTupleValue implements TupleValue {

  public final FieldValueContainerSet containers;
  public Resetable resetable[];
  
  public AbstractTupleValue( FieldValueContainerSet containers ) {
    this.containers = containers;
  }
  
  public void reset( ) {
    if ( resetable == null )
      return;
    for ( int i = 0; i < resetable.length;  i++ )
      resetable[i].reset( );
  }

  @Override
  public FieldValue field(int i) {
    return containers.field(i);
  }

  @Override
  public FieldValue field(String name) {
    FieldSchema field = schema().field(name);
    if (field == null)
      return null;
    return field(field.index());
  }

}

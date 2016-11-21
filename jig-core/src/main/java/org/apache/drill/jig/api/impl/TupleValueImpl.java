package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.accessor.FieldAccessor.Resetable;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.container.FieldValueContainerSet;
import org.apache.drill.jig.util.JigUtilities;

public class TupleValueImpl implements TupleValue {

  protected final TupleSchema schema;
  protected final FieldValueContainerSet containers;
  public Resetable resetable[];
  private final int size;

  public TupleValueImpl( TupleSchema schema, FieldValueContainerSet containers ) {
    this.schema = schema;
    this.containers = containers;
    size = containers.size( );
  }

  @Override
  public TupleSchema schema() {
    return schema;
  }

  public void reset( ) {
    if ( resetable == null )
      return;
    for ( int i = 0; i < resetable.length;  i++ )
      resetable[i].reset( );
  }

  @Override
  public FieldValue field(int i) {
    FieldSchema field = schema.field( i );
    return getFieldValue( field );
  }

  @Override
  public FieldValue field(String name) {
    FieldSchema field = schema().field(name);
    if (field == null)
      return null;
    return getFieldValue( field );
  }

  protected FieldValue getFieldValue( FieldSchema field ) {
    return containers.field(field.index());
  }

  @Override
  public String toString( ) {
    StringBuilder buf = new StringBuilder( );
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.visualizeLn(buf, 1, "schema", schema);
    JigUtilities.visualizeLn(buf, 1, "containers", containers);
    JigUtilities.indent(buf,  1 );
    buf.append( "resetable: " );
    if ( resetable == null  ||  resetable.length == 0 ) {
      buf.append( "(none)\n" );
    } else {
      buf.append( "\n" );
      for ( int i = 0;  i < resetable.length; i++ ) {
        JigUtilities.indent(buf, 2);
        buf.append( resetable[i].getClass( ).getSimpleName( ) );
        buf.append( "\n" );
      }
    }
    visualizeBody( buf, 0 );
    JigUtilities.indent( buf, 1 );
    buf.append( "]" );
    return buf.toString();
  }

  protected void visualizeBody(StringBuilder buf, int indent) {
  }
}

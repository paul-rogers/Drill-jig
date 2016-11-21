package org.apache.drill.jig.container;

import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.util.JigUtilities;
import org.apache.drill.jig.util.Visualizable;

public class FieldValueContainerSet implements Visualizable {

  private final FieldValueContainer containers[];

  public FieldValueContainerSet( FieldValueContainer containers[] ) {
    this.containers = containers;
  }

  public int size( ) { return containers.length; }

  public FieldValue field( int index ) {
    if ( index < 0  ||  containers.length <= index )
      return null;
    return containers[index].get();
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( " containers:\n" );
    for ( int i = 0;  i < containers.length;  i++ ) {
      JigUtilities.indent( buf, indent + 1);
      containers[i].visualize( buf, indent + 1 );
      buf.append( "\n" );
    }
    JigUtilities.indent( buf, indent + 1);
    buf.append( "]" );
  }
}

package org.apache.drill.jig.types;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.util.JigUtilities;
import org.apache.drill.jig.util.Visualizable;

/**
 * Provides a cache of reusable field values. Used to implement Variant
 * fields.
 */

public class FieldValueCache implements Visualizable {

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

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "factory = " );
    buf.append( factory.toString( ) );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "values:\n" );
    for ( int i = 0;  i < values.length;  i++ ) {
      JigUtilities.indent( buf, indent + 2 );
      buf.append( i );
      buf.append( " (" );
      buf.append( DataType.values()[i] );
      buf.append( "): " );
      if ( values[i] == null ) {
        buf.append( "null" );
      } else {
        values[i].visualize( buf, indent + 3 );
      }
      buf.append( "\n" );
    }
    JigUtilities.indent( buf, indent );
    buf.append( "]" );
  }
}

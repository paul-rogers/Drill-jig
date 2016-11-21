package org.apache.drill.jig.util;

public class JigUtilities {

  public static boolean isBlank( String str ) {
    return str == null ||  str.trim().isEmpty();
  }

  public static void indent( StringBuilder buf, int level ) {
    for ( int i = 0;  i < level;  i++ ) {
      buf.append( "  " );
    }
  }

  public static void objectHeader( StringBuilder buf, Object obj ) {
    buf.append( "[" );
    buf.append( obj.getClass().getSimpleName() );
  }

  public static void visualize( StringBuilder buf, int level, String label, Object obj ) {
    buf.append( label );
    buf.append( " = " );
    if ( obj == null )
      buf.append( "null" );
    else if ( obj instanceof Visualizable ) {
      ((Visualizable) obj ).visualize( buf, level + 1 );
    } else {
      buf.append( obj.toString() );
    }
  }

  public static void visualizeLn( StringBuilder buf, int level, String label, Object obj ) {
    indent( buf, level );
    visualize( buf, level, label, obj );
    buf.append( "\n" );
  }

  public static void quote(StringBuilder buf, Object value) {
    if ( value == null ) {
      buf.append( "null" );
    } else {
      if ( value instanceof String ) {
        buf.append( "\"" );
      }
      buf.append( value );
      if ( value instanceof String ) {
        buf.append( "\"" );
      }
    }
  }

}

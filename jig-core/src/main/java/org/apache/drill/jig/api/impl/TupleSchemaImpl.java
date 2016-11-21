package org.apache.drill.jig.api.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.exception.SchemaException;
import org.apache.drill.jig.util.JigUtilities;
import org.apache.drill.jig.util.Visualizable;

public class TupleSchemaImpl implements TupleSchema, Visualizable
{
  protected List<FieldSchema> fields = new ArrayList<>( );
  protected Map<String,FieldSchema> nameIndex = new HashMap<>( );

  @Override
  public int count() {
    return fields.size();
  }

  @Override
  public FieldSchema field(int i) {
    if ( i < 0  &&  i >= fields.size( ) ) {
      return null; }
    return fields.get( i );
  }

  @Override
  public FieldSchema field(String name) {
    return nameIndex.get( name );
  }

  public void add(FieldSchemaImpl field)
  {
    field.setIndex( fields.size( ) );
    fields.add( field );
    String name = field.name();
    if ( nameIndex.containsKey( name ) ) {
      throw new SchemaException( "Duplicate field name: " + name );
    }
    nameIndex.put( name, field );
  }

  public Iterable<FieldSchema> fields( ) {
    return fields;
  }

  @Override
  public String toString( ) {
    StringBuilder buf = new StringBuilder( );
    buf.append( "[Tuple Schema " );
    for ( int i = 0;  i < fields.size();  i++ ) {
      if ( i > 0 )
        buf.append( "\n  " );
      buf.append( fields.get( i ).toString( ) );
    }
    buf.append( " ]" );
    return buf.toString();
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    for ( FieldSchema field : fields ) {
      JigUtilities.indent(buf, indent + 1 );
      buf.append( field.index() );
      buf.append( ": " );
      buf.append( field.toString( ) );
      buf.append( "\n" );
    }
    JigUtilities.indent(buf, indent + 1 );
    buf.append( "]" );
  }
}
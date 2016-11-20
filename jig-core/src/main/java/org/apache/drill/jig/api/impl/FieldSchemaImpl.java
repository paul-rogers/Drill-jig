package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;

/**
 * Standard implementation of a field schema. Subclasses exist for
 * certain field types: {@link ArrayFieldSchemaImpl} for arrays,
 * {@link TupleFieldSchemaImpl} for tuples.
 */

public class FieldSchemaImpl implements FieldSchema
{
  protected int index;
  protected final String name;
  protected final boolean isNullable;
  protected final DataType type;
  protected int length;

  public FieldSchemaImpl( String name, DataType type, boolean isNullable ) {
    this.name = name;
    this.isNullable = isNullable;
    this.type = type;
  }

  @Override
  public String name() { return name; }

  @Override
  public int index() { return index; }

  public void setIndex( int index ) { this.index = index; }

  @Override
  public DataType type() { return type; }

  @Override
  public boolean nullable() { return isNullable; }

  @Override
  public String getDisplayType() {
    return type.displayName() + (isNullable ? "?" : "");
  }

  @Override
  public int getLength( ) { return length; }

  @Override
  public String toString( ) {
    StringBuilder buf = new StringBuilder( );
    buf.append( "[Field Schema: " );
    buildString( buf );
    buf.append( "]" );
    return buf.toString();
  }

  protected void buildString(StringBuilder buf) {
    buf.append( "name=" );
    buf.append( name );
    buf.append( ", type=" );
    buf.append( type );
    buf.append( ", nullable=" );
    buf.append( isNullable );
  }

  @Override
  public FieldSchema element() {
    return null;
  }

  @Override
  public TupleSchema schema() {
    return null;
  }
}

package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;

public class FieldSchemaImpl implements FieldSchema
{
  protected int index;
  protected final String name;
  protected final boolean isNullable;
  protected DataType type;
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

}
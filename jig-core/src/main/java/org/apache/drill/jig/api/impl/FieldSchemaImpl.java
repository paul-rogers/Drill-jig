package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.Cardinality;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;

public class FieldSchemaImpl implements FieldSchema
{
  protected int index;
  protected final String name;
  protected final Cardinality cardinality;
  protected DataType type;
  protected int length;

  public FieldSchemaImpl( String name, DataType type, Cardinality cardinality ) {
    this.name = name;
    this.cardinality = cardinality;
    this.type = type;
  }
  
  @Override
  public String getName() { return name; }

  @Override
  public int getIndex() { return index; }
  
  public void setIndex( int index ) { this.index = index; }

  @Override
  public DataType getType() { return type; }

  @Override
  public Cardinality getCardinality() { return cardinality; }

  @Override
  public String getDisplayType() {
    return type.getDisplayName() + cardinality.getDisplaySuffix();
  }
  
  @Override
  public int getLength( ) { return length; }

}
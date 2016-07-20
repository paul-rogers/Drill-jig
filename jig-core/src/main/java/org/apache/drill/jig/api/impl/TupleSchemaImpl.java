package org.apache.drill.jig.api.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.SchemaException;
import org.apache.drill.jig.api.TupleSchema;

public class TupleSchemaImpl implements TupleSchema
{
  protected List<FieldSchema> fields = new ArrayList<>( );
  protected Map<String,FieldSchema> nameIndex = new HashMap<>( );

  @Override
  public int getCount() {
    return fields.size();
  }

  @Override
  public FieldSchema getField(int i) {
    if ( i < 0  &&  i >= fields.size( ) ) {
      return null; }
    return fields.get( i );
  }

  @Override
  public FieldSchema getField(String name) {
    return nameIndex.get( name );
  }

  public void add(FieldSchemaImpl field)
  {
    field.setIndex( fields.size( ) );
    fields.add( field );
    String name = field.getName();
    if ( nameIndex.containsKey( name ) ) {
      throw new SchemaException( "Duplicate field name: " + name );
    }
    nameIndex.put( name, field );
  }
  
  public Iterable<FieldSchema> fields( ) {
    return fields;
  }
}
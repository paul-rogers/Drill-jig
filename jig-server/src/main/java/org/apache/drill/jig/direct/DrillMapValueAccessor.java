package org.apache.drill.jig.direct;

import java.util.Map;

import org.apache.drill.jig.accessor.CachedObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.TupleValueAccessor;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.container.FieldValueContainerSet;

/**
 * Drill maps are actually tuples: they have a defined schema.
 * The MapAccessor materializes Drill maps as a Java Map. But,
 * the vector batch schema provides a schema for the map. We combine the
 * schema along with the Java Map object to create a Tuple Value to
 * represent the Drill Map.
 */

public class DrillMapValueAccessor implements TupleValueAccessor, TupleValue {

  private final ObjectAccessor mapObjAccessor;
  private final TupleSchema schema;
  private final FieldValueContainerSet containerSet;
  private final CachedObjectAccessor valueObjAccessor;
  
  public DrillMapValueAccessor( TupleSchema schema, FieldValueContainerSet containerSet,
      ObjectAccessor mapAccessor, CachedObjectAccessor valueObjAccessor ) {
    this.schema = schema;
    this.containerSet = containerSet;
    mapObjAccessor = mapAccessor;
    this.valueObjAccessor = valueObjAccessor;
  }
  
  @Override
  public boolean isNull() {
    return mapObjAccessor.isNull();
  }

  @Override
  public TupleValue getTuple() {
    return this;
  }

  @Override
  public TupleSchema schema() {
    return schema;
  }

  @Override
  public FieldValue field(int i) {
    FieldSchema field = schema.field( i );
    return getFieldValue( field );
  }
  
  @SuppressWarnings("unchecked")
  private FieldValue getFieldValue( FieldSchema field ) {
    if ( field == null )
      return null;
    Map<String,Object> map = ((Map<String,Object>) mapObjAccessor.getObject());
    Object fieldValue = map.get( field.name( ) );
    valueObjAccessor.bind( fieldValue );
    return containerSet.field( field.index( ) );
  }

  @Override
  public FieldValue field(String name) {
    return getFieldValue( schema.field( name ) );
  }
}

package org.apache.drill.jig.direct;

import java.util.Map;

import org.apache.drill.jig.accessor.CachedObjectAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.TupleValueAccessor;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.impl.TupleValueImpl;
import org.apache.drill.jig.container.FieldValueContainerSet;
import org.apache.drill.jig.util.JigUtilities;

/**
 * Drill maps are actually tuples: they have a defined schema.
 * The MapAccessor materializes Drill maps as a Java Map. But,
 * the vector batch schema provides a schema for the map. We combine the
 * schema along with the Java Map object to create a Tuple Value to
 * represent the Drill Map.
 */

public class DrillMapValueAccessor extends TupleValueImpl implements TupleValueAccessor {

  private final ObjectAccessor mapObjAccessor;
  private final CachedObjectAccessor valueObjAccessor;

  public DrillMapValueAccessor( TupleSchema schema, FieldValueContainerSet containerSet,
      ObjectAccessor mapAccessor, CachedObjectAccessor valueObjAccessor ) {
    super( schema, containerSet );
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

  @SuppressWarnings("unchecked")
  @Override
  protected FieldValue getFieldValue( FieldSchema field ) {
    if ( field == null )
      return null;
    Map<String,Object> map = ((Map<String,Object>) mapObjAccessor.getObject());
    Object fieldValue = map.get( field.name( ) );
    valueObjAccessor.bind( fieldValue );
    return containers.field( field.index( ) );
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.visualizeLn(buf, indent + 1, "schema", schema);
    JigUtilities.visualizeLn(buf, indent + 1, "map accessor", mapObjAccessor);
    JigUtilities.visualizeLn(buf, indent + 1, "value accessor", valueObjAccessor);
    JigUtilities.visualizeLn(buf, indent + 1, "container set", containers);
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "]" );
  }
}

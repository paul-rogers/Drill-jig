package org.apache.drill.jig.accessor;

import java.util.Collection;
import java.util.Map;

import org.apache.drill.jig.accessor.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ValueObjectAccessor;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.container.VariantFieldValueContainer;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.util.JigUtilities;

/**
 * MapValueAccessor (and corresponding MapValue) for a Java
 * {@link Map} object provided by an {@link ObjectAccessor}.
 */

public class JavaMapAccessor implements MapValueAccessor, MapValue, ValueObjectAccessor {

  private ObjectAccessor mapAccessor;
  private final VariantFieldValueContainer valueContainer;
  private final CachedObjectAccessor valueAccessor;

  public JavaMapAccessor( FieldValueFactory factory ) {
    valueContainer = new VariantFieldValueContainer( factory );
    valueAccessor = new CachedObjectAccessor( );
    valueContainer.bind( factory.newVariantObjectAccessor( valueAccessor ) );
  }

  public JavaMapAccessor( ObjectAccessor accessor, FieldValueFactory factory ) {
    this( factory );
    this.mapAccessor = accessor;
  }

  public void bind( ObjectAccessor accessor ) {
    this.mapAccessor = accessor;
  }

  @Override
  public boolean isNull() {
    return mapAccessor.isNull();
  }

  @Override
  public int size() {
    @SuppressWarnings("rawtypes")
    Map map = getMapObject( );
    if ( map == null )
      return 0;
    return map.size();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Collection<String> keys() {
    return getMapObject( ).keySet();
  }

  @Override
  public FieldValue get(String key) {
    valueAccessor.bind( getMapObject( ).get( key ) );
    return valueContainer.get();
  }

  @Override
  public MapValue getMap() {
    return this;
  }

  @SuppressWarnings("rawtypes")
  private Map getMapObject( ) {
    return (Map) mapAccessor.getObject();
  }

  @Override
  public Object getValue() {
    return mapAccessor.getObject();
  }

  @Override
  public void visualize(StringBuilder buf, int indent) {
    JigUtilities.objectHeader( buf, this );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent + 1 );
    buf.append( "map accessor = " );
    mapAccessor.visualize( buf, indent + 2 );
    buf.append( "\n" );
    buf.append( "value accessor = " );
    valueAccessor.visualize( buf, indent + 2 );
    buf.append( "\n" );
    buf.append( "value container = " );
    valueContainer.visualize( buf, indent + 2 );
    buf.append( "\n" );
    JigUtilities.indent( buf, indent );
    buf.append( "]" );
  }
}

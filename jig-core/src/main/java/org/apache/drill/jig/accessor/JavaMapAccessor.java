package org.apache.drill.jig.accessor;

import java.util.Collection;
import java.util.Map;

import org.apache.drill.jig.accessor.BoxedAccessor.VariantBoxedAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.MapValueAccessor;
import org.apache.drill.jig.accessor.FieldAccessor.ValueObjectAccessor;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.container.VariantFieldValueContainer;
import org.apache.drill.jig.types.FieldValueFactory;

/**
 * MapValueAccessor (and corresponding MapValue) for a Java
 * {@link Map} object provided by an {@link ObjectAccessor}.
 */

public class JavaMapAccessor implements MapValueAccessor, MapValue, ValueObjectAccessor {

  private ObjectAccessor accessor;
  private final VariantFieldValueContainer valueContainer;
  private final CachedObjectAccessor valueAccessor;

  public JavaMapAccessor( FieldValueFactory factory ) {
    valueContainer = new VariantFieldValueContainer( factory );
    valueAccessor = new CachedObjectAccessor( );
    valueContainer.bind( factory.newVariantObjectAccessor( valueAccessor ) );
  }

  public JavaMapAccessor( ObjectAccessor accessor, FieldValueFactory factory ) {
    this( factory );
    this.accessor = accessor;
  }
  
  public void bind( ObjectAccessor accessor ) {
    this.accessor = accessor;
  }
  
  @Override
  public boolean isNull() {
    return accessor.isNull();
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
    return (Map) accessor.getObject();
  }

  @Override
  public Object getValue() {
    return accessor.getObject();
  }
}

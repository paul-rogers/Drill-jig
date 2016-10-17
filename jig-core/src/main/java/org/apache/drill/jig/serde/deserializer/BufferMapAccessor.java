package org.apache.drill.jig.serde.deserializer;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.serde.deserializer.BufferScalarAccessor.BufferMemberAccessor;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldAccessor.Resetable;
import org.apache.drill.jig.types.FieldValueCache;
import org.apache.drill.jig.types.FieldValueFactory;

public class BufferMapAccessor implements ObjectAccessor, Resetable {

  private Object cached;
  protected int index;
  protected TupleSetDeserializer deserializer;
  private final FieldValueCache valueCache;
  private BufferMemberAccessor valueAccessor;
  
  public BufferMapAccessor( FieldValueFactory factory ) {
    valueCache = new FieldValueCache( factory );
    valueAccessor = new BufferMemberAccessor( );
  }
  
  public void bind( TupleSetDeserializer deserializer, int index ) {
    this.deserializer = deserializer;
    this.index = index;
    valueAccessor.bind( deserializer, 0 );
  }
  
  @Override
  public void reset() {
    cached = null;
  }

  @Override
  public boolean isNull() {
    return deserializer.isNull( index );
  }
  
  protected void seek( ) {
    deserializer.seek( index );
  }
  
  @Override
  public Object getObject() {
    if ( cached == null )
      cached = buildMap( );
    return cached;
  }

  private Object buildMap() {
    deserializer.seek( index );
    TupleReader reader = deserializer.reader( );
    reader.readInt( ); // Skip field length
    int count = reader.readIntEncoded();
    Map<String,Object> map = new HashMap<>( );
    for ( int i = 0;  i < count;  i++ ) {
      String key = reader.readString();
      DataType type = DataType.typeForCode( reader.readByte() );
      AbstractFieldValue value = valueCache.get( type );
      value.bind( valueAccessor );
      map.put( key, value.getValue( ) );
    }
    return map;
  }

}

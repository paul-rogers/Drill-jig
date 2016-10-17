package org.apache.drill.jig.serde.deserializer;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.serde.deserializer.BufferScalarAccessor.BufferMemberAccessor;
import org.apache.drill.jig.types.AbstractFieldValue;
import org.apache.drill.jig.types.FieldValueCache;
import org.apache.drill.jig.types.FieldValueFactory;

public class BufferMapAccessor extends BufferStructureAccessor {

  private final FieldValueCache valueCache;
  private BufferMemberAccessor valueAccessor;
  
  public BufferMapAccessor( FieldValueFactory factory ) {
    valueCache = new FieldValueCache( factory );
    valueAccessor = new BufferMemberAccessor( );
  }
  
  @Override
  public void bind( TupleSetDeserializer deserializer, int index ) {
    super.bind( deserializer, index);
    valueAccessor.bind( deserializer, 0 );
  }
  
  @Override
  public Object buildStructure() {
    TupleReader reader = deserializer.reader( );
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

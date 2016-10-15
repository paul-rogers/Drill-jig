package org.apache.drill.jig.serde.deserializer;

import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.serde.deserializer.BufferScalarAccessor.*;
import org.apache.drill.jig.types.DataDef;
import org.apache.drill.jig.types.DataDef.ScalarDef;
import org.apache.drill.jig.types.FieldValueContainer;
import org.apache.drill.jig.types.FieldValueContainerSet;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.NullAccessor;

public class TupleBuilder {
  
  private TupleSetDeserializer deserializer;
  private FieldValueFactory factory;

  public TupleBuilder( TupleSetDeserializer deserializer ) {
    this.deserializer = deserializer;
    factory = new FieldValueFactory( );
  }
  
  public TupleValue build( TupleSchema schema ) {
    int n = schema.count();
    DataDef defs[] = new DataDef[n];
    for ( int i = 0; i < n;  i++ ) {
      defs[i] = buildDef( schema.field( i ) );
    }
    for ( int i = 0;  i < n;  i++ ) {
      defs[i].build( factory );
    }
    FieldValueContainer containers[] = new FieldValueContainer[n];
    for ( int i = 0;  i < n;  i++ ) {
      containers[i] = defs[i].container;
    }
    FieldValueContainerSet containerSet = new FieldValueContainerSet( containers );
    return new BufferTupleValue( containerSet, deserializer );
  }
  
  private DataDef buildDef(FieldSchema field) {
    switch ( field.type() ) {
    case BOOLEAN:
    case DECIMAL:
    case FLOAT32:
    case FLOAT64:
    case INT16:
    case INT32:
    case INT64:
    case INT8:
    case STRING:
      return buildScalar( field );
    case LIST:
      return buildList( field );
    case MAP:
      return buildMap( field );
    case NULL:
    case UNDEFINED:
      return buildNull( field );
    case NUMBER:
    case VARIANT:
      return buildVariant( field );
    case BLOB:
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case UTC_DATE_TIME:
    case TUPLE:
      throw new IllegalStateException( "Unsupported data type: " + field.type( ) );
    default:
      throw new IllegalStateException( "Unexpected data type: " + field.type( ) );
    
    }
  }

  private DataDef buildScalar(FieldSchema field) {
    BufferScalarFieldAccessor accessor = new BufferScalarFieldAccessor( );
    accessor.bind( deserializer, field.index() );
    return new ScalarDef( field.type(), field.nullable(), accessor );
  }

  private DataDef buildNull(FieldSchema field) {
    return new ScalarDef( field.type(), field.nullable(), new NullAccessor( ) );
  }

  private DataDef buildVariant(FieldSchema field) {
    BufferVariantFieldAccessor accessor = new BufferVariantFieldAccessor( );
    accessor.bind( deserializer, field.index() );
    return new ScalarDef( field.type(), field.nullable(), accessor );
  }

  private DataDef buildList(FieldSchema field) {
    // TODO Auto-generated method stub
    return null;
  }

  private DataDef buildMap(FieldSchema field) {
    // TODO Auto-generated method stub
    return null;
  }

}

package org.apache.drill.jig.serde.deserializer;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.impl.AbstractTupleValue;
import org.apache.drill.jig.serde.deserializer.BufferScalarAccessor.*;
import org.apache.drill.jig.serde.deserializer.BufferArrayAccessor.*;
import org.apache.drill.jig.types.BoxedAccessor;
import org.apache.drill.jig.types.DataDef;
import org.apache.drill.jig.types.DataDef.*;
import org.apache.drill.jig.types.FieldAccessor.ObjectAccessor;
import org.apache.drill.jig.types.FieldAccessor.Resetable;
import org.apache.drill.jig.types.FieldValueContainer;
import org.apache.drill.jig.types.FieldValueContainerSet;
import org.apache.drill.jig.types.FieldValueFactory;
import org.apache.drill.jig.types.JavaArrayAccessor;
import org.apache.drill.jig.types.JavaArrayAccessor.*;
import org.apache.drill.jig.types.JavaMapAccessor;
import org.apache.drill.jig.types.NullAccessor;
import org.apache.drill.jig.types.BoxedAccessor.VariantBoxedAccessor;

public class TupleBuilder {
  
  private TupleSetDeserializer deserializer;
  private FieldValueFactory factory;
  private List<Resetable> resets = new ArrayList<>( );

  public TupleBuilder( TupleSetDeserializer deserializer ) {
    this.deserializer = deserializer;
    factory = new FieldValueFactory( );
  }
  
  public AbstractTupleValue build( TupleSchema schema ) {
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
    BufferTupleValue tuple = new BufferTupleValue( schema, containerSet );
    if ( ! resets.isEmpty() ) {
      tuple.resetable = new Resetable[resets.size()];
      resets.toArray( tuple.resetable );
    }
    return tuple;
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

  public enum ListType { PRIMITIVE, TYPED_OBJECT, VARIANT };
  
  private DataDef buildList(FieldSchema field) {
    FieldSchema member = field.member();
    DataType memberType = member.type();
    DataDef memberDef = buildDef( member );
    BufferArrayAccessor accessor;
    ListType listType;
    switch ( memberType ) {
    case BOOLEAN:
      accessor = new BooleanArrayAccessor( );
      listType = ListType.PRIMITIVE;
      break;
    case DECIMAL:
      accessor = new DecimalArrayAccessor( );
      listType = ListType.TYPED_OBJECT;
      break;
    case FLOAT32:
      accessor = new Float32ArrayAccessor( );
      listType = ListType.PRIMITIVE;
      break;
    case FLOAT64:
      accessor = new Float64ArrayAccessor( );
      listType = ListType.PRIMITIVE;
      break;
    case INT16:
      accessor = new Int16ArrayAccessor( );
      listType = ListType.PRIMITIVE;
      break;
    case INT32:
      accessor = new Int32ArrayAccessor( );
      listType = ListType.PRIMITIVE;
      break;
    case INT64:
      accessor = new Int64ArrayAccessor( );
      listType = ListType.PRIMITIVE;
      break;
    case INT8:
      accessor = new Int8ArrayAccessor( );
      listType = ListType.PRIMITIVE;
      break;
    case LIST:
      break;
    case STRING:
      accessor = new StringArrayAccessor( );
      listType = ListType.TYPED_OBJECT;
      break;
    case NUMBER:
    case VARIANT:
      accessor = new VariantArrayAccessor( factory );
      listType = ListType.VARIANT;
      break;
    case BLOB:
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case MAP:
    case NULL:
    case TUPLE:
    case UNDEFINED:
    case UTC_DATE_TIME:
      throw new IllegalStateException( "Unsupported array type: " + member.type() );
    default:
      throw new IllegalStateException( "Unexpected array type: " + member.type( ) );   
    }
    resets.add( accessor );
    JavaArrayAccessor arrayAccessor;
    switch ( listType ) {
    case PRIMITIVE:
      arrayAccessor = new PrimitiveArrayAccessor( accessor, member.type( ) );
      break;
    case TYPED_OBJECT:
      arrayAccessor = new ObjectArrayAccessor( accessor );
      if ( memberType.isVariant() ) {
        accessor = new VariantBoxedAccessor( objAccessor, factory );
      } else {
        accessor = new BoxedAccessor( objAccessor );
      }      memberDef.buildFieldAccessor( (ObjectAccessor) arrayAccessor.memberAccessor(), factory );
      break;
    case VARIANT:
      break;
    default:
      break;
    
    }
    return new ListDef( field.nullable(), new Ja);
  }

  private DataDef buildList(FieldSchema field) {
    FieldSchema member = field.member();
    DataType memberType = member.type();
    DataDef memberDef = buildDef( member );
    BufferArrayAccessor accessor;
    ListType listType;
    switch ( memberType ) {
    case BOOLEAN:
      return primitiveArray( new BooleanArrayAccessor( ), field );
    case DECIMAL:
      return typedObjectArray( new DecimalArrayAccessor( ), field );
    case FLOAT32:
      return primitiveArray( new Float32ArrayAccessor( ), field );
    case FLOAT64:
      return primitiveArray( new Float64ArrayAccessor( ), field );
    case INT16:
      return primitiveArray( new Int16ArrayAccessor( ), field );
    case INT32:
      return primitiveArray( new Int32ArrayAccessor( ), field );
    case INT64:
      return primitiveArray( new Int64ArrayAccessor( ), field );
    case INT8:
      return primitiveArray( new Int8ArrayAccessor( ), field );
    case LIST:
      return arrayArray( field );
    case STRING:
      return typedObjectArray( new StringArrayAccessor( ), field );
    case NUMBER:
    case VARIANT:
      return variantArray( field );
    case BLOB:
    case DATE:
    case DATE_TIME_SPAN:
    case LOCAL_DATE_TIME:
    case MAP:
    case NULL:
    case TUPLE:
    case UNDEFINED:
    case UTC_DATE_TIME:
      throw new IllegalStateException( "Unsupported array type: " + member.type() );
    default:
      throw new IllegalStateException( "Unexpected array type: " + member.type( ) );   
    }
  }

  private DataDef primitiveArray( BufferArrayAccessor arrayAccessor, FieldSchema field ) {
    resets.add( arrayAccessor );
    FieldSchema member = field.member();
    PrimitiveArrayAccessor accessor = new PrimitiveArrayAccessor( arrayAccessor, member.type() );
    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), accessor.memberAccessor( ) );
    return new ListDef( field.nullable(), memberDef, accessor );
  }

  private DataDef typedObjectArray(BufferArrayAccessor arrayAccessor,
      FieldSchema field) {
    resets.add( arrayAccessor );
    FieldSchema member = field.member();
    ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( arrayAccessor );
    BoxedAccessor memberAccessor = new BoxedAccessor( (ObjectAccessor) objArrayAccessor.memberAccessor() );
    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), memberAccessor );
    return new ListDef( field.nullable(), memberDef, objArrayAccessor );
  }

  private DataDef variantArray(FieldSchema field) {
    FieldSchema member = field.member();
    VariantArrayAccessor arrayAccessor = new VariantArrayAccessor( factory );
    resets.add( arrayAccessor );
    ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( arrayAccessor );
    BoxedAccessor memberAccessor = new VariantBoxedAccessor( (ObjectAccessor) objArrayAccessor.memberAccessor(), factory );
    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), memberAccessor );
    return new ListDef( field.nullable(), memberDef, objArrayAccessor );
  }

  private DataDef arrayArray(FieldSchema field) {
    // TODO Auto-generated method stub
    return null;
  }

  private DataDef buildMap(FieldSchema field) {
    BufferMapAccessor accessor = new BufferMapAccessor( factory );
    resets.add( accessor );
    return new MapDef( field.nullable(), new JavaMapAccessor( accessor, factory ) );
  }

}

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

  private DataDef buildList(FieldSchema field) {
    FieldSchema member = field.member();
    switch ( member.type() ) {
    case BOOLEAN:
      return primitiveArray( new BooleanArrayAccessor( ), field );
    case DECIMAL:
      return typedObjectArray( new DecimalArrayAccessor( member.nullable() ), field );
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
      return typedObjectArray( new StringArrayAccessor( member.nullable() ), field );
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

  /**
   * Jig types that correspond to primitive Java types are deserialized into a
   * Java primitive array of the proper type. Then we use a Java primitive array
   * accessor to present the primitive Java array as a Jig array.
   * 
   * @param arrayAccessor array deserializer of the proper type
   * @param field
   * @return
   */
  
  private DataDef primitiveArray( BufferArrayAccessor arrayAccessor, FieldSchema field ) {
    
    // The array accessor builds a primitive Java array of the proper type
    // and presents it as a Java object.
    
    arrayAccessor.bind( deserializer, field.index() );
    
    // The deserialized array is cached per-tuple, so add a reset to clear the
    // cached value.
    
    resets.add( arrayAccessor );
    
    // Build a Java primitive array accessor to present our primitive array as
    // a Jig array.
    
    FieldSchema member = field.member();
    PrimitiveArrayAccessor accessor = new PrimitiveArrayAccessor( arrayAccessor, member.type() );
    
    // Define the member and array data elements. The definitions will build the
    // field values and field value containers.
    
    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), accessor.memberAccessor( ) );
    return new ListDef( field.nullable(), memberDef, accessor );
  }

  /**
   * Represents a serialized array as an array of Java objects. Used for Strings
   * and Decimals. The array is deserialized into a Java object array, which is
   * then presented as an object to a Java Object array accessor, which presents
   * the array to the client using the Jig Array API.
   * 
   * @param arrayAccessor accessor that deserializes teh array to the proper
   * type
   * @param field
   * @return
   */
  private DataDef typedObjectArray(BufferArrayAccessor arrayAccessor,
      FieldSchema field) {
    
    // The provided array accessor deserializes the array into a Java object
    // array, and presents it as a Java object.
    
    arrayAccessor.bind( deserializer, field.index() );
    
    // The array deserializer caches the deserialized Java array, so add it
    // as a per-tuple rest.
    
    resets.add( arrayAccessor );
    
    // Create the accessor that presents the Java object array using the Jig
    // Array API.
    
    ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( arrayAccessor );
    
    // The member values are "boxed" Java objects. (Not really boxed for String
    // and decimal, but the idea also works for boxed Integers, etc.)
    // The important bit is that we expect all members to be of the
    // declared member type
    
    BoxedAccessor memberAccessor = new BoxedAccessor( (ObjectAccessor) objArrayAccessor.memberAccessor() );
    
    // Build the data element definitions that will build the array and member
    // field values and field value containers.
    
    FieldSchema member = field.member();
    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), memberAccessor );
    return new ListDef( field.nullable(), memberDef, objArrayAccessor );
  }

  /**
   * Create a variant array. A variant array is an array that holds any kind
   * of scalar value. Each value is written as a type/value pair. (Nulls are
   * written as the NULL type with no value.) This array is deserialized as a
   * Java object array what is then wrapped in a Java array accessor which
   * presents the variant array as a Jig array.
   * 
   * @param field
   * @return
   */
  
  private DataDef variantArray(FieldSchema field) {
    
    // Create an accessor to deserialize a variant array. Since variants
    // carry their own type information, no type information is require
    // for array members.
    
    FieldSchema member = field.member();
    VariantArrayAccessor arrayAccessor = new VariantArrayAccessor( factory );
    arrayAccessor.bind( deserializer, field.index() );
    
    // The constructed object array is cached, so add a reset to clear
    // the cache on each tuple.
    
    resets.add( arrayAccessor );
    
    // The variant array accessor is presented as an object accessor to
    // a Java object array accessor.
    
    ObjectArrayAccessor objArrayAccessor = new ObjectArrayAccessor( arrayAccessor );
    
    // The member accessor is one that reads type/object pairs as
    // "boxed" Java objects. (That is, ints are stored as Integers, etc.)
    
    BoxedAccessor memberAccessor = new VariantBoxedAccessor( (ObjectAccessor) objArrayAccessor.memberAccessor(), factory );
    
    // Create the data element definition that will create the FieldValue API
    // objects and the corresponding variant container to provide the correct
    // FieldValue object for each data item type.
    
    DataDef memberDef = new ScalarDef( member.type(), member.nullable(), memberAccessor );
    return new ListDef( field.nullable(), memberDef, objArrayAccessor );
  }

  private DataDef arrayArray(FieldSchema field) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Maps are deserialized into a Java map, then we use a Java map accessor to
   * present the Java map as a Jig map.
   * 
   * @param field
   * @return
   */
  
  private DataDef buildMap(FieldSchema field) {
    
    // The buffer map accessor deserializes the map to a Java map,
    // which is presented as a Java object.
    
    BufferMapAccessor accessor = new BufferMapAccessor( factory );
    accessor.bind( deserializer, field.index() );
    
    // The buffer map accessor caches the value for each tuple,
    // add a reset to clear the cached value on each new tuple.
    
    resets.add( accessor );
    
    // Use a Java map accessor to present the deserialized map to the
    // client via the Jig map API.
    
    return new MapDef( field.nullable(), new JavaMapAccessor( accessor, factory ) );
  }

}

package org.apache.drill.jig.serde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.FieldValue;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.ArrayResultCollection;
import org.apache.drill.jig.extras.array.Batch;
import org.apache.drill.jig.serde.deserializer.SimpleBufferResultSet;
import org.apache.drill.jig.serde.serializer.TupleSetSerializer;

public class SerdeTestUtils {

  public static Map<String,Object> map1( ) {
    Map<String,Object> map1 = new HashMap<>( );
    map1.put( "one", 1 );
    map1.put( "two", 2L );
    map1.put( "three", 3d );
    map1.put( "four", BigDecimal.ONE );
    map1.put( "five", null );
    map1.put( "six", "mumble" );
    return map1;
  }

  public static Map<String,Object> map2( ) {
    Map<String,Object> map2 = new HashMap<>( );
    map2.put( "a", "a-value" );
    map2.put( "b", 10 );
    return map2;
  }
  
  public static ByteBuffer loadBuffer(TupleSet tuples) throws JigException {
    ByteBuffer buf = ByteBuffer.allocate( 4096 );
    TupleSetSerializer serializer = new TupleSetSerializer( tuples.schema() );
    while ( tuples.next() ) {
      serializer.serializeTuple( buf, tuples.tuple() );
    }
    buf.flip();
    return buf;
  }
  
  public static ResultCollection serdeResults( ResultCollection results ) throws JigException {
    assertTrue( results.next( ) );
    TupleSet tuples = results.tuples( );    

    ByteBuffer buf = loadBuffer( tuples );
    
    // Pass ownership of schema from input to output. Works because the
    // schema is, essentially, immutable.
    
    SimpleBufferResultSet bufferSet = new SimpleBufferResultSet( tuples.schema(), buf );
    results.close( );
    return bufferSet;
  }
  
  public static ResultCollection serdeBatch( Batch batch ) throws JigException {
    ArrayResultCollection results = new ArrayResultCollection( batch );
    return serdeResults( results );
  }
  
  public static ResultCollection serdeBatches( Batch batches[] ) throws JigException {
    ArrayResultCollection results = new ArrayResultCollection( batches );
    return serdeResults( results );
  }
  
  public static void validateSerde( Batch expected ) throws JigException {
    validateResults( expected, serdeBatch( expected ) );
  }

  public static void validateResults( Batch expected, ResultCollection actual ) throws JigException {
    ResultCollection expectedResults = new ArrayResultCollection( expected );
    validateResults( expectedResults, actual );
  }

  public static void validateResults(ResultCollection expected,
      ResultCollection actual) throws JigException {
    for ( ; ; ) {
      boolean expectedMore = expected.next();
      boolean actualMore = actual.next( );
      assertEquals( expectedMore, actualMore );
      if ( ! expectedMore )
        break;
      validateTuples( expected.tuples(), actual.tuples() );
    }
    expected.close();
    actual.close();
  }

  public static void validateTuples(TupleSet expected, TupleSet actual) throws JigException {
    TupleSchema expectedSchema = expected.schema( );
    TupleSchema actualSchema = actual.schema( );
    validateSchema( expectedSchema, actualSchema );
    for ( ; ; ) {
      boolean expectedMore = expected.next();
      boolean actualMore = actual.next( );
      assertEquals( expectedMore, actualMore );
      if ( ! expectedMore )
        break;
      validateTuple( expected.tuple(), actual.tuple() );
    }
  }

  public static void validateSchema(TupleSchema expected,
      TupleSchema actual) {
    assertEquals( expected.count(), actual.count() );
    for ( int i = 0;  i < expected.count();  i++ ) {
      validateFieldSchema( expected.field( i ), actual.field( i ) );
    }
  }

  public static void validateFieldSchema(FieldSchema expected,
      FieldSchema actual) {
    assertEquals( expected.name(), actual.name() );
    assertEquals( expected.type(), actual.type() );
    assertEquals( expected.nullable(), actual.nullable() );
    if ( expected.type() == DataType.LIST )
      validateFieldSchema( expected.element(), actual.element() );
  }

  public static void validateTuple(TupleValue expected, TupleValue actual) {
    TupleSchema schema = expected.schema( );
    for ( int i = 0;  i < schema.count();  i++ ) {
      validateField( schema.field(i), expected.field(i), actual.field(i) );
    }
  }

  public static void validateField(FieldSchema schema, FieldValue expected,
      FieldValue actual) {
    assertEquals( expected.isNull(), actual.isNull() );
    assertEquals( expected.type(), actual.type( ) );
    
    // Sanity check: non-nullable fields should not be null.
    // This will fail only if the expected results are also wrong.
    
    assertTrue( schema.nullable() || ! actual.isNull() );
    
    // Sanity check. The container system should ensure that null
    // values are presented as the NULL field value.
    
    if ( expected.isNull() ) {
      assertEquals( DataType.NULL, actual.type( ) );
      return;
    }
    
    // Sanity check: Non-variant, non-null values should have the
    // type declared in the schema.
    
    else if ( ! schema.type().isVariant() )
      assertEquals( schema.type(), actual.type( ) );
    
    switch ( schema.type() ) {
    case BLOB:
      throw new IllegalStateException( "Unsupported type: " + schema.type() );
    case LIST:
      validateList( schema, expected.getArray(), actual.getArray() );
      break;
    case MAP:
      validateMap( expected.getMap(), actual.getMap() );
      break;
    case NULL:
    case UNDEFINED:
      break;
    case TUPLE:
      throw new IllegalStateException( "Unsupported type: " + schema.type() );
    default:
      assertEquals( expected.getValue(), actual.getValue( ) );   
    }
  }

  public static void validateList(FieldSchema schema, ArrayValue expected, ArrayValue actual) {
    assertEquals( expected.size(), actual.size() );
    FieldSchema memberSchema = schema.element();
    for ( int i = 0;  i < expected.size( );  i++ ) {
      validateField( memberSchema, expected.get(i), actual.get(i) );
    }    
  }

  public static void validateMap(MapValue expected, MapValue actual) {
    assertEquals( expected.size(), actual.size() );
    List<String> expectedKeys = new ArrayList<>( );
    expectedKeys.addAll( expected.keys() );
    Collections.sort( expectedKeys );
    List<String> actualKeys = new ArrayList<>( );
    actualKeys.addAll( actual.keys() );
    Collections.sort( actualKeys );
    assertEquals( expectedKeys, actualKeys );
    for ( String key : expectedKeys ) {
      FieldValue expectedValue = expected.get( key );
      FieldValue actualValue = actual.get( key );
      assertEquals( expectedValue.isNull(), actualValue.isNull() );
      assertEquals( expectedValue.type(), actualValue.type() );
      assertEquals( expectedValue.getValue(), actualValue.getValue() );
    }
  }
}

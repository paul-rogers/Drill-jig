package org.apache.drill.jig.extras.json;

import static org.junit.Assert.*;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.MapValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.JigException;
import org.junit.Test;

public class TestResultSet {

  @Test
  public void testBasics() throws JigException {
    String input =
        "{ \"a\": 10, \"b\": \"str\" }\n" +
        "{ \"a\": 20, \"b\": \"mumble\" }\n" +
        "{ \"a\": 30, \"b\": \"foo\" }\n";
    JsonResultCollection results = new JsonResultCollection( input );
    assertTrue( results.next() );
    
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    assertNotNull( schema );
    assertEquals( 2, schema.count() );
    assertEquals( "a", schema.field(0).name() );
    assertEquals( DataType.INT64, schema.field(0).type( ) );
    assertFalse( schema.field(0).nullable() );
    
    assertEquals( "b", schema.field(1).name() );
    assertEquals( DataType.STRING, schema.field(1).type( ) );
    assertFalse( schema.field(1).nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertSame( schema, tuple.schema() );
    
    assertEquals( 10, tuple.field(0).getInt() );
    assertEquals( "str", tuple.field(1).getString() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( 20, tuple.field(0).getInt() );
    assertEquals( "mumble", tuple.field(1).getString() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( 30, tuple.field(0).getInt() );
    assertEquals( "foo", tuple.field(1).getString() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testMap() throws JigException {
    String input =
        "{ \"a\": 10, \"b\": { \"c\": \"str\", \"d\": 100 } }\n" +
        "{ \"a\": 20, \"b\": { \"c\": \"mumble\", \"d\": 200 } }\n" +
        "{ \"a\": 30, \"b\": { \"c\": \"foo\", \"d\": 300 } }\n";
    
    JsonResultCollection results = new JsonResultCollection( input );
    assertTrue( results.next() );
    
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    assertEquals( 2, schema.count() );
    assertEquals( DataType.MAP, schema.field(1).type( ) );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    
    assertEquals( 10, tuple.field(0).getInt() );
    MapValue map = tuple.field(1).getMap();
    assertNotNull( map );
    assertNotNull( map.get("c") );
    assertEquals( DataType.STRING, map.get("c").type() );
    assertEquals( "str", map.get("c").getString() );
    assertNotNull( map.get("d") );
    assertEquals( DataType.INT64, map.get("d").type() );
    assertEquals( 100, map.get("d").getInt() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();    
    assertEquals( 20, tuple.field(0).getInt() );
    map = tuple.field(1).getMap();
    assertEquals( "mumble", map.get("c").getString() );
    assertEquals( 200, map.get("d").getInt() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();    
    assertEquals( 30, tuple.field(0).getInt() );
    map = tuple.field(1).getMap();
    assertEquals( "foo", map.get("c").getString() );
    assertEquals( 300, map.get("d").getInt() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testFlattened() throws JigException {
    String input =
        "{ \"a\": 10, \"b\": { \"c\": \"str\", \"d\": 100 } }\n" +
        "{ \"a\": 20, \"b\": { \"c\": \"mumble\", \"d\": 200 } }\n" +
        "{ \"a\": 30, \"b\": { \"c\": \"foo\", \"d\": 300 } }\n";
    
    JsonResultCollection results = new JsonResultCollectionBuilder( )
        .forString( input )
        .flatten( )
        .build( );
    assertTrue( results.next() );
    
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    assertEquals( 3, schema.count() );
    assertEquals( DataType.STRING, schema.field(1).type( ) );
    assertEquals( DataType.INT64, schema.field(2).type( ) );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    
    assertEquals( 10, tuple.field(0).getInt() );
    assertEquals( "str", tuple.field("b.c").getString() );
    assertEquals( 100, tuple.field("b.d").getInt() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();    
    assertEquals( "mumble", tuple.field("b.c").getString() );
    assertEquals( 200, tuple.field("b.d").getInt() );
    
    assertTrue( tupleSet.next() );
    assertEquals( "foo", tuple.field("b.c").getString() );
    assertEquals( 300, tuple.field("b.d").getInt() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testVariant() throws JigException {
    String input =
        "{ \"a\": 10, \"b\": \"str\" }\n" +
        "{ \"a\": 20, \"b\": 200 }\n" +
        "{ \"a\": 30, \"b\": null }\n";
    JsonResultCollection results = new JsonResultCollection( input );
    assertTrue( results.next() );
    
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    assertNotNull( schema );
    assertEquals( 2, schema.count() );
    assertEquals( "a", schema.field(0).name() );
    assertEquals( DataType.INT64, schema.field(0).type( ) );
    assertFalse( schema.field(0).nullable() );
    
    assertEquals( "b", schema.field(1).name() );
    assertEquals( DataType.VARIANT, schema.field(1).type( ) );
    assertTrue( schema.field(1).nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertSame( schema, tuple.schema() );
    
    assertEquals( 10, tuple.field(0).getInt() );
    assertEquals( "str", tuple.field(1).getString() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( 20, tuple.field(0).getInt() );
    assertEquals( 200, tuple.field(1).getInt() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( 30, tuple.field(0).getInt() );
    assertTrue( tuple.field(1).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }


  @Test
  public void testArray() throws JigException {
    String input =
        "{ \"a\": 10, \"b\": [ 101, 102 ] }\n" +
        "{ \"a\": 20, \"b\": [ 201, 202 ] }\n" +
        "{ \"a\": 30, \"b\": [ ] }\n";
    JsonResultCollection results = new JsonResultCollection( input );
    assertTrue( results.next() );
    
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    assertNotNull( schema );
    assertEquals( 2, schema.count() );
    assertEquals( "a", schema.field(0).name() );
    assertEquals( DataType.INT64, schema.field(0).type( ) );
    assertFalse( schema.field(0).nullable() );
    
    assertEquals( "b", schema.field(1).name() );
    assertEquals( DataType.LIST, schema.field(1).type( ) );
    assertFalse( schema.field(1).nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertSame( schema, tuple.schema() );
    
    assertEquals( 10, tuple.field(0).getInt() );
    ArrayValue array = tuple.field(1).getArray();
    assertNotNull( array );
    assertEquals( 2, array.size() );
    assertEquals( 101, array.get(0).getInt() );
    assertEquals( 102, array.get(1).getInt() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( 20, tuple.field(0).getInt() );
    assertEquals( 2, array.size() );
    assertEquals( 201, array.get(0).getInt() );
    assertEquals( 202, array.get(1).getInt() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertEquals( 30, tuple.field(0).getInt() );
    assertEquals( 0, array.size() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

}

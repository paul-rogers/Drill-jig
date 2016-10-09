package org.apache.drill.jig.api.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.jig.api.ArrayValue;
import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.extras.array.ArrayResultCollection;
import org.apache.drill.jig.extras.array.Batch;
import org.junit.Test;

public class TestLists {

  /**
   * Lists provide no type information as the parameteried type is
   * "erased" at runtime. So type information is inferred from members.
   * Test a list all of the same type.
   * 
   * @throws JigException
   */
  
  @Test
  public void testTypedScalarList( ) throws JigException {
    List<Object> list1 = new ArrayList<>( );
    list1.add( "first" );
    list1.add( "second" );
    List<Object> list2 = new ArrayList<>( );
    list2.add( "a" );
    list2.add( "b" );
    list2.add( "c" );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { list1 },
          { list2 }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.STRING, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareLists( list1, array );
    assertEquals( DataType.STRING, array.get(0).type() );
    assertEquals( list1.get( 0 ), array.get(0).getString() );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    Helpers.compareLists( list2, tuple.field( 0 ).getArray() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }
  
  /**
   * Test a list of a single type, but that includes nulls.
   * 
   * @throws JigException
   */
  
  @Test
  public void testTypedNullableScalarList( ) throws JigException {
    List<Object> list1 = new ArrayList<>( );
    list1.add( "first" );
    list1.add( "second" );
    list1.add( null );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { list1 },
         }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.STRING, member.type() );
    assertTrue( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareLists( list1, array );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  /**
   * Test a list field that contains null lists.
   * 
   * @throws JigException
   */

  @Test
  public void testNullableTypedScalarList( ) throws JigException {
    List<Object> list1 = new ArrayList<>( );
    list1.add( "first" );
    list1.add( "second" );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { list1 },
          { null }
         }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertTrue( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.STRING, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    ArrayValue array = tuple.field( 0 ).getArray();
    Helpers.compareLists( list1, array );  
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    assertTrue( tuple.field( 0 ).isNull() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  /**
   * Test an empty list. No type information is available.
   * 
   * @throws JigException
   */
  
  @Test
  public void testEmptyList( ) throws JigException {
    List<Object> list1 = new ArrayList<>( );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { list1 },
         }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.getTuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( "col", field.name() );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.member();
    assertNotNull( member );
    assertEquals( DataType.UNDEFINED, member.type() );
    assertFalse( member.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( 0, array.size() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

}

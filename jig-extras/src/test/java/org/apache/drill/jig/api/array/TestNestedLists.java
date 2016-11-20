package org.apache.drill.jig.api.array;

import static org.junit.Assert.*;

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

public class TestNestedLists {

  @Test
  public void testListOfList() throws JigException {
    List<Object> list1 = new ArrayList<>( );
    list1.add( "first" );
    list1.add( "second" );
    List<Object> list2 = new ArrayList<>( );
    list2.add( "a" );
    list2.add( "b" );
    list2.add( "c" );
    List<Object> parent = new ArrayList<>( );
    parent.add( list1 );
    parent.add( list2 );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { parent },
          { new ArrayList<Object>( ) }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.element();
    assertNotNull( member );
    assertEquals( DataType.LIST, member.type() );
    assertFalse( member.nullable() );
    
    FieldSchema member2 = member.element();
    assertNotNull( member2 );
    assertEquals( DataType.STRING, member2.type() );
    assertFalse( member2.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( 2, array.size() );
    assertEquals( DataType.LIST, array.get( 0 ).type() );
    assertEquals( DataType.LIST, array.get( 0 ).type() );
    Helpers.compareLists( list1, array.get( 0 ).getArray( ) );
    Helpers.compareLists( list2, array.get( 1 ).getArray( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    array = tuple.field( 0 ).getArray();
    assertEquals( 0, array.size() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testListOfArray() throws JigException {
    List<Object> parent = new ArrayList<>( );
    parent.add( new String[] { "first", "second" } );
    parent.add( new String[] { "a", "b", "c" } );
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { parent },
          { new ArrayList<Object>( ) }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.element();
    assertNotNull( member );
    assertEquals( DataType.LIST, member.type() );
    assertFalse( member.nullable() );
    
    FieldSchema member2 = member.element();
    assertNotNull( member2 );
    assertEquals( DataType.STRING, member2.type() );
    assertFalse( member2.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( 2, array.size() );
    assertEquals( DataType.LIST, array.get( 0 ).type() );
    assertEquals( DataType.LIST, array.get( 0 ).type() );
    Helpers.compareArrays( parent.get( 0 ), array.get( 0 ).getArray( ) );
    Helpers.compareArrays( parent.get( 1 ), array.get( 1 ).getArray( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    array = tuple.field( 0 ).getArray();
    assertEquals( 0, array.size() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }

  @Test
  public void testArrayOfList() throws JigException {
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
          { new List[] { list1, list2 } },
          { new List[] { } }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.element();
    assertNotNull( member );
    assertEquals( DataType.LIST, member.type() );
    assertFalse( member.nullable() );
    
    FieldSchema member2 = member.element();
    assertNotNull( member2 );
    assertEquals( DataType.STRING, member2.type() );
    assertFalse( member2.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( 2, array.size() );
    assertEquals( DataType.LIST, array.get( 0 ).type() );
    assertEquals( DataType.LIST, array.get( 0 ).type() );
    Helpers.compareLists( list1, array.get( 0 ).getArray( ) );
    Helpers.compareLists( list2, array.get( 1 ).getArray( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    array = tuple.field( 0 ).getArray();
    assertEquals( 0, array.size() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }


  @Test
  public void testArrayOfArray() throws JigException {
    String[] array1 = new String[] { "first", "second" };
    String[] array2 = new String[] { "a", "b", "c" };
    Batch batch = new Batch(
        new String[] { "col" },
        new Object[][] {
          { new String[][] { array1, array2 } },
          { new String[][] { } }
        }
      );
    
    ResultCollection results = new ArrayResultCollection( batch );
    assertTrue( results.next() );
    TupleSet tupleSet = results.tuples();
    
    TupleSchema schema = tupleSet.schema();
    FieldSchema field = schema.field( 0 );
    assertEquals( DataType.LIST, field.type() );
    assertFalse( field.nullable() );
    
    FieldSchema member = field.element();
    assertNotNull( member );
    assertEquals( DataType.LIST, member.type() );
    assertFalse( member.nullable() );
    
    FieldSchema member2 = member.element();
    assertNotNull( member2 );
    assertEquals( DataType.STRING, member2.type() );
    assertFalse( member2.nullable() );
    
    assertTrue( tupleSet.next() );
    TupleValue tuple = tupleSet.tuple();
    assertEquals( DataType.LIST, tuple.field( 0 ).type() );
    ArrayValue array = tuple.field( 0 ).getArray();
    assertEquals( 2, array.size() );
    assertEquals( DataType.LIST, array.get( 0 ).type() );
    assertEquals( DataType.LIST, array.get( 0 ).type() );
    Helpers.compareArrays( array1, array.get( 0 ).getArray( ) );
    Helpers.compareArrays( array2, array.get( 1 ).getArray( ) );
    
    assertTrue( tupleSet.next() );
    tuple = tupleSet.tuple();
    array = tuple.field( 0 ).getArray();
    assertEquals( 0, array.size() );
    
    assertFalse( tupleSet.next() );
    assertFalse( results.next() );
    results.close( );
  }


}

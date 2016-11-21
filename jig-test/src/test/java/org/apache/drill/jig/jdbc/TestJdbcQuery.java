package org.apache.drill.jig.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.drill.jig.server.DrillPressContext;
import org.apache.drill.jig.server.EmbeddedDrillPress;
import org.junit.Test;

public class TestJdbcQuery
{

  @Test
  public void test() throws SQLException {
    DrillPressContext context = new DrillPressContext( )
        .direct( "localhost" );
    EmbeddedDrillPress drillPress = new EmbeddedDrillPress( context )
        .start( );
    
    JigDriver driver = new JigDriver( );
    Connection conn = driver.connect( "jdbc:jig:", null );
    
    Statement stmt = conn.createStatement();
    ResultSet results = stmt.executeQuery( "SELECT employee_id, full_name, salary FROM cp.`employee.json` LIMIT 20" );
    ResultSetMetaData md = results.getMetaData();
    assertEquals( 3, md.getColumnCount() );
    
    assertEquals( "employee_id", md.getColumnName( 1 ) );
    assertEquals( "employee_id", md.getColumnLabel( 1 ) );
    assertEquals( Types.BIGINT, md.getColumnType( 1 ) );
    assertEquals( "Int-64", md.getColumnTypeName( 1 ) );
//    assertEquals( "employee.json", md.getTableName( 1 ) );
//    assertEquals( "cp", md.getCatalogName( 1 ) );
    assertFalse( md.isCurrency( 1 ) );
    
    assertEquals( "full_name", md.getColumnName( 2 ) );
    assertEquals( "full_name", md.getColumnLabel( 2 ) );
    assertEquals( Types.VARCHAR, md.getColumnType( 2 ) );
    assertEquals( "String", md.getColumnTypeName( 2 ) );
//    assertEquals( "employee.json", md.getTableName( 2 ) );
//    assertEquals( "cp", md.getCatalogName( 21 ) );
    assertFalse( md.isCurrency( 2 ) );
    
    assertEquals( "salary", md.getColumnName( 3 ) );
    assertEquals( "salary", md.getColumnLabel( 3 ) );
    assertEquals( Types.DOUBLE, md.getColumnType( 3 ) );
    assertEquals( "Float-64", md.getColumnTypeName( 3 ) );
//    assertEquals( "employee.json", md.getTableName( 3 ) );
//    assertEquals( "cp", md.getCatalogName( 3 ) );
    assertFalse( md.isCurrency( 3 ) );
    
    assertEquals( 1, results.findColumn( "employee_id") );
    assertEquals( 2, results.findColumn( "full_name") );
    assertEquals( 3, results.findColumn( "salary") );
    
    assertTrue( results.isBeforeFirst() );
    assertFalse( results.isAfterLast() );
    assertEquals( ResultSet.TYPE_FORWARD_ONLY, results.getType() );
    assertEquals( 0, results.getRow() );
    int row = 0;
    while ( results.next() ) {
      row++;
      assertEquals( row, results.getRow( ) );
      assertFalse( results.isBeforeFirst() );
      assertFalse( results.isAfterLast() );
      System.out.print( results.getLong( 1 ) );
      assertFalse( results.wasNull() );
      System.out.print( ", " );
      System.out.print( results.getString( 2 ) );
      assertFalse( results.wasNull() );
      System.out.print( ", " );
      System.out.println( results.getDouble( 3 ) );
      assertFalse( results.wasNull() );
    }
    assertFalse( results.isBeforeFirst() );
    assertTrue( results.isAfterLast() );
    
    results.close();
    conn.close();
    drillPress.stop();
  }

}

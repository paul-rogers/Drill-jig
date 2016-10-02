package org.apache.drill.jig.drillpress;

import static org.junit.Assert.assertTrue;

import java.io.StringWriter;

import org.apache.drill.jig.api.DrillConnection;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.Statement;
import org.apache.drill.jig.client.ConnectionFactory;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.test.CompareFiles;
import org.apache.drill.jig.util.RowDisplay;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRemoteApi
{
  static EmbeddedDrillPress drillPress;
  
  /**
   * One-time setup. Only one Drillpress can run per process.
   */
  
  @BeforeClass
  public static void setup( ) {
    DrillPressContext context = new DrillPressContext( )
        .direct( "localhost" );
    drillPress = new EmbeddedDrillPress( context )
        .start( );
  }
  
  @AfterClass
  public static void shutDown( ) {
    drillPress.stop();
  }
  
  @Test
  public void test() throws JigException {
    DrillConnection conn = new ConnectionFactory( )
      .connect( )
      .login( );
    
    String stmtText = "SELECT * FROM cp.`employee.json` LIMIT 20";
    Statement stmt = conn.prepare( stmtText );
    ResultCollection results = stmt.execute();
    StringWriter out = new StringWriter( );
//    RowDisplay.printResults( results );
    RowDisplay.printResults( results, out );
    assertTrue( CompareFiles.compareResource( "/employees-20.txt", out.toString() ) );
    results.close();
    conn.close();
  }
}

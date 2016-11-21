package org.apache.drill.jig.server;

import org.apache.drill.jig.api.DrillConnection;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.Statement;
import org.apache.drill.jig.client.ConnectionFactory;
import org.apache.drill.jig.exception.JigException;
import org.apache.drill.jig.util.RowDisplay;

/**
 * Connect to a running Drillpress on the local machine on the
 * default port and run a simple query. This is not a JUnit
 * test because we can't easily start the Drillpress server
 * from this code.
 */

public class ExerciseDrillpress {
  
  public static void main( String args[] ) {
    try {
      new ExerciseDrillpress( ).run( );
    } catch (JigException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void run() throws JigException {
    DrillConnection conn = new ConnectionFactory( )
        .toHost( "localHost" )
        .connect( )
        .login( );
      
      String stmtText = "SELECT * FROM cp.`employee.json` LIMIT 20";
      Statement stmt = conn.prepare( stmtText );
      ResultCollection results = stmt.execute();
//      StringWriter out = new StringWriter( );
      RowDisplay.printResults( results );
//      RowDisplay.printResults( results, out );
//      assertTrue( CompareFiles.compareResource( "/employees-20.txt", out.toString() ) );
      results.close();
      conn.close();
  }

}

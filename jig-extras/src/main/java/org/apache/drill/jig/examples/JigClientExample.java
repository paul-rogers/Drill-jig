package org.apache.drill.jig.examples;

import org.apache.drill.jig.api.DrillConnection;
import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.client.ConnectionFactory;
import org.apache.drill.jig.drillpress.DrillPressContext;
import org.apache.drill.jig.drillpress.EmbeddedDrillPress;
import org.apache.drill.jig.exception.JigException;

/**
 * Illustrates the very basics of using Jig to query Drill.
 * Establishes a connection, runs a query and prints the results.
 * The Jig server can be embedded (the default) or run
 * separately.
 */

public class JigClientExample {

  public static void main(String[] args) {
    JigClientExample example = new JigClientExample( );
    
    try {
      // To run DrillPress in this process
      example.runEmbedded();
      
      // To connect to a separate Drillpress process.
      
      //example.example();
    } catch (JigException e) {
      
      // Something went wrong. Most likely Drill is not
      // up, the wrong host is specified, or a login is
      // required.
      
      e.printStackTrace();
    }
  }
  
  /**
   * Runs the example using an embedded Jig server. The Jig server
   * connects to Drill directly on the local host. Feel free to
   * to tinker with the context setup if your Drill is on another
   * host.
   * 
   * @throws JigException
   */
  
  public void runEmbedded( ) throws JigException
  {
    // Gather the information needed to connect to Drill
    // (or optionally start it embedded.)
    
    DrillPressContext context = new DrillPressContext( )
        .direct( "localhost" );   // Connect to the named host on the
                                  // default user port.
    
    // Start a Drillpress server in this process.
    
    EmbeddedDrillPress drillPress = new EmbeddedDrillPress( context )
        .start( );
    
    // Do the example
    
    example( );
    
    // Stop the Drillpress server.
    
    drillPress.stop();
  }

  /**
   * Run the example against a Drillpress (Jig server) running on
   * the local host (embedded if called from the above method.)
   * Issue a query and print the results.
   * 
   * @throws JigException
   */

  public void example() throws JigException {
    
    // Log into Drill via the Jig server
    
    DrillConnection conn = new ConnectionFactory( )
        .toHost( "localhost" )        // Jig server host (default port)
        .connect( )                   // Connect to server (login is separate)
//        .login( "fred", "secret" ); // If Drill requires a user name and password
        .login( );                    // If no username/password is required
    
    // Execute a query and get the results.
    
    ResultCollection results = conn.prepare( "SELECT * FROM cp.`employee.json` LIMIT 20" )
        .execute();
    
    // Iterate over the result sets (may be multiple if a schema
    // change occurs.)
    // The ResultCollection is a collection of result sets.
    
    while( results.next() ) {
      
      // Get the next result set: a collection of tuples (records).
      
      TupleSet tupleSet = results.tuples();
      
      // Each tuple set has a distinct schema.
      // Display the column names
      
      TupleSchema schema = tupleSet.schema( );
      int n = schema.count();
      for ( int i = 0; i < n;  i++ ) {
        if ( i > 0 ) System.out.print( ", " );
        System.out.print( schema.field(i).name());
      }
      System.out.println( );
      
      // Iterate over the tuples (rows, records) in this tuple set.
      // Print each field naively, by converting it to a Java object.
      // Not pretty or efficient, but fine for an example.
      
      while ( tupleSet.next() ) {
        TupleValue tuple = tupleSet.tuple();
        for ( int i = 0; i < n;  i++ ) {
          if ( i > 0 ) System.out.print( ", " );
          System.out.print(
              tuple.field(i)   // Get the field object
              .getValue() );   // Get the value as a Java object.
        }
        System.out.println( );
      }
    }
    results.close();
    conn.close();
  }
}

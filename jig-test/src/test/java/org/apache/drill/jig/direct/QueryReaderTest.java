package org.apache.drill.jig.direct;

import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.jig.api.AlterSessionKeys;
import org.apache.drill.jig.direct.DrillClientContext;
import org.apache.drill.jig.direct.DrillConnectionFactory;
import org.apache.drill.jig.direct.DrillContextFactory;
import org.apache.drill.jig.direct.DrillSession;
import org.apache.drill.jig.direct.VectorRecord;
import org.apache.drill.jig.direct.VectorRecordReader;
import org.apache.drill.jig.test.CompareFiles;
import org.junit.Test;

/**
 * Test the implementation of the adapter between Drill
 * value vectors and Jig tuples.
 */

public class QueryReaderTest
{
  @Test
  public void testEmbedded() throws Exception {
    System.out.println( "testEmbedded" );
    new DrillContextFactory( )
        .withEmbeddedDrillbit( )
        .build( )
        .startEmbedded( );
    DrillSession session = new DrillConnectionFactory( )
        .drillbit( "localhost" )
        .connect( );
    
    session.alterSession( AlterSessionKeys.MAX_WIDTH_PER_NODE, 2 );
    
    StringWriter out = new StringWriter( );
    testQueryPrinter( session, new PrintWriter( out ) );
//    System.out.print( out );
    assertTrue( CompareFiles.compareResource( "/direct-emp-20.txt", out.toString() ) );
    
    session.close();
    DrillClientContext.instance( ).stopEmbeddedDrillbit( );
  }

  private void testQueryPrinter(DrillSession session, PrintWriter out ) {
    String stmt = "SELECT * FROM cp.`employee.json` LIMIT 20";
    VectorRecordReader reader = new VectorRecordReader( session, stmt );
    int count = 0;
    outer:
    for ( ; ; ) {
      switch ( reader.next() ) {
      case EOF:
        break outer;
      case SCHEMA:
        showSchema( reader.getSchema(), out );
        break;
      case RECORD:
        count++;
        if ( count < 20 ) {
          showRecord( reader.getRecord(), out );
        }
        break;
      }
    }
    reader.close();
    out.flush();
  }

  private void showSchema(BatchSchema schema, PrintWriter out) {
    out.println( "Index, Path, Nullable, Type, Mode, Class" );
    int i = 0;
    for ( MaterializedField field : schema ) {
      out.print( i );
      out.print( ", " );
      out.print( field.getPath() );
      out.print( ", " );
      out.print( field.isNullable() );
      out.print( ", " );
      out.print( field.getType().getMinorType().name() );
      out.print( ", " );
      out.print( field.getDataMode().name() );
      out.print( ", " );
      out.print( field.getValueClass().getSimpleName() );
      out.println( );
      i++;
    }
    String sep = "";
    for ( MaterializedField field : schema ) {
      out.print( sep );
      out.print( field.getPath() );
      sep = ", ";
    }
    out.println( );
  }

  private void showRecord(VectorRecord record, PrintWriter out) {
    String sep = "";
    for ( int i = 0;  i < record.getFieldCount();  i++ ) {
      Object value = record.getValue( i );
      String wrap = "";
      if ( value != null  &&  value instanceof String ) { wrap = "'"; }
      out.print( sep );
      out.print( wrap );
      out.print( value == null ? "null" : value.toString() );
      out.print( wrap );
      sep = ", ";
    }
    out.println( );
  }

}

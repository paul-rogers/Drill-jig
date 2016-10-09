package org.apache.drill.jig.util;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

import org.apache.drill.jig.api.ResultCollection;
import org.apache.drill.jig.api.TupleValue;
import org.apache.drill.jig.api.TupleSchema;
import org.apache.drill.jig.api.TupleSet;
import org.apache.drill.jig.exception.JigException;

public class RowDisplay
{
  public static void printResults( ResultCollection results ) throws JigException {
    try {
      printResults( results,
          new OutputStreamWriter( System.out, "UTF-8" ) );
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException( e );
    }
  }
  
  public static void printResults( ResultCollection results, Writer out ) throws JigException {
    printResults( results, new PrintWriter( out ) );
  }
  
  public static void printResults( ResultCollection results, PrintWriter out ) throws JigException {
    while( results.next() ) {
      TupleSet tupleSet = results.getTuples();
      RowDisplay.printSchema( tupleSet.schema(), out );
      while ( tupleSet.next() )
        RowDisplay.printRow( tupleSet.tuple(), out );
    }
    out.flush( );
  }
  
  public static void printSchema(TupleSchema schema, PrintWriter out) {
    int n = schema.count();
    for ( int i = 0; i < n;  i++ ) {
      if ( i > 0 )
        out.print( ", " );
      out.print( schema.field(i).name());
    }
    out.println( );
  }

  public static void printRow(TupleValue tuple, PrintWriter out) {
    int n = tuple.schema().count();
    for ( int i = 0; i < n;  i++ ) {
      if ( i > 0 )
        out.print( ", " );
      Object value = tuple.field(i).getValue();
      String disp;
      if ( value == null )
        disp = "<null>";
      else {
        disp = value.toString();
        if ( disp.contains( " " ) || disp.contains( "," ) ) {
          disp = "\"" + disp + "\"";
        }
      }
      out.print( disp );
    }
    out.println( );
  }
}

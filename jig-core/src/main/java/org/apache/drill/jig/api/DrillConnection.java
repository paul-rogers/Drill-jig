package org.apache.drill.jig.api;

/**
 * Connection to the Drill proxy. Each connection is
 * single-threaded and can execute one statement (query) at a time.
 * An application can, however, have any number of connections open
 * at the same time.
 */

public interface DrillConnection extends AutoCloseable
{
  void alterSession( String key, int value ) throws JigException;
  void alterSession( String key, String value ) throws JigException;
  int execute( String stmt ) throws JigException;
  Statement prepare( String text ) throws JigException;
  @Override
  void close( );
}

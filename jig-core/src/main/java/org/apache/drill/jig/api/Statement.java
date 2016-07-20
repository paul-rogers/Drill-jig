package org.apache.drill.jig.api;

/**
 * Represents a statement. A statement has a life cycle
 * as follows:
 * <ul>
 * <li>Create</li>
 * <li>Prepare (not yet)</li>
 * <li>Set parameters (not yet)</li>
 * <li>Execute</li>
 * <li>Fetch results (if a query)<li>
 * <li>close</li>
 * <p>
 * The statement has no results if the query returns no rows.
 * <p>
 * Closing a statement before all rows is read is legal; doing so
 * simply cancels the query in Drill.
 */

public interface Statement extends AutoCloseable
{
  ResultCollection execute( ) throws JigException;
  @Override
  void close( ) throws JigException;
}
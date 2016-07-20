package org.apache.drill.jig.api;

/**
 * Provides access to a single record (or nested structure.) The tuple
 * accessor is <i>not</i> a tuple; it is a sliding window into a
 * set of tuples. That is, a tuple accessor cannot be stored in a
 * list. The contents of one cannot be compared with another. Each
 * tuple set may have only one tuple accessor that follows the
 * iteration through the tuple set.
 * <p>
 * Tuple accessors provide access to the tuple's data, but does
 * not materialize that data. It is the responsibility of the caller
 * to provide a persistent storage mechanism if desired.
 */

public interface TupleAccessor
{
 TupleSchema getSchema( );
 FieldAccessor getField( int i );
 FieldAccessor getField( String name );
}
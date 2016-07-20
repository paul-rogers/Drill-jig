package org.apache.drill.jig.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.apache.drill.jig.api.JigException;
import org.apache.drill.jig.api.ResultCollection;

public class JigJdbcStatement implements Statement
{
  private JigJdbcConnection conn;
  private org.apache.drill.jig.api.Statement stmt;
  private ResultCollection results;
  private JigJdbcResultSet resultSet;
  
  public JigJdbcStatement( JigJdbcConnection conn ) {
    this.conn = conn;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if ( iface.isAssignableFrom( org.apache.drill.jig.api.Statement.class ) )
      return (T) stmt;
    if ( iface.isAssignableFrom( ResultCollection.class ) )
      return (T) results;
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom( org.apache.drill.jig.api.Statement.class )  ||
           iface.isAssignableFrom( ResultCollection.class );
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    close( );
    try {
      stmt = conn.getJigConnection( ).prepare( sql );
      results = stmt.execute();
      if ( ! results.next() )
        return null;
      resultSet = new JigJdbcResultSet( this, results.getTuples() );
      conn.startResultSet( this );
      return resultSet;
    } catch (JigException e) {
      throw translateException( e );
    }
  }

  private JigWrapperException translateException(JigException e) {
    // TODO Better translation
    return new JigWrapperException( e );
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new UnsupportedException( "updates" );
  }

  @Override
  public void close() throws SQLException {
    if ( stmt == null )
      return;
    try {
      conn.onStatementClose( this );
      stmt.close();
    } catch (JigException e) {
      throw translateException( e );
    }
    finally {
      stmt = null;
      if ( resultSet != null )
        resultSet.close( );
      results = null;
      resultSet = null;
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    // Drill has no max field size. Also, Drill doesn't do updates...
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new UnsupportedException( "setMaxFieldSize" );
  }

  @Override
  public int getMaxRows() throws SQLException {
    // Drill does big data, there is no fetch limit
    return 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new UnsupportedException( "setMaxRows" );
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getQueryTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void cancel() throws SQLException {
    close( );
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // Drill has no warnings at the present time
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    // No-op because not supported by Drill
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    try {
      stmt = conn.getJigConnection( ).prepare( sql );
      results = stmt.execute();
      
      // Returns true because the first result is a result set.
      // The result set may be empty, but we'll return true anyway
      // to avoid having to fetch that result set here.
      
      return true;
    } catch (JigException e) {
      throw translateException( e );
    }
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    if ( resultSet == null )
      return false;
    try {
      if ( ! results.next() ) {
        resultSet = null;
        return false;
      }
    } catch (JigException e) {
      close( );
      throw translateException( e );
    }
    resultSet = new JigJdbcResultSet( this, results.getTuples() );
    return true;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    if ( direction != ResultSet.FETCH_FORWARD )
      throw new UnsupportedException( "Reverse fetch" );

  }

  @Override
  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // Ignored because fetches are based on buffer size, not row count.
  }

  @Override
  public int getFetchSize() throws SQLException {
    // Implementation-specific value saying we don't speak row count,
    // only buffer size.
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    // Drill is read only.
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    // Drill is read-only
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw updateUnsupported( );
  }

  @Override
  public void clearBatch() throws SQLException {
    // Silently ignore
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new UnsupportedException( "batches" );
  }

  @Override
  public Connection getConnection() throws SQLException {
    return conn;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    if ( current == Statement.KEEP_CURRENT_RESULT )
      throw new UnsupportedException( "multiple open result sets" );
    // The current options don't make sense for Drill.
    return getMoreResults( );
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    // Not supported, Drill doesn't generate keys.
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw updateUnsupported( );
  }

  private UnsupportedException updateUnsupported() {
    return new UnsupportedException( "updates" );
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw updateUnsupported( );
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw updateUnsupported( );
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    // Drill does not generate keys, so ignore the keys option.
    return execute( sql );
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    // Key column indexes ignored since Drill does not generate keys.
    return execute( sql );
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    // Key column names ignored since Drill does not generate keys.
    return execute( sql );
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    // Commit does not close cursors because Drill does not support transactions.
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return stmt == null;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    // Ignore: Drill statements are not poolable
  }

  @Override
  public boolean isPoolable() throws SQLException {
    // Drill statements are not poolable
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    // Ignore: Drill statements and result sets are the same thing.
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    // Drill statements and result sets are the same thing.
    return true;
  }

}

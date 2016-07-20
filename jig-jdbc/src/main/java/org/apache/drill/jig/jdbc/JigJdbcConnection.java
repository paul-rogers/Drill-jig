package org.apache.drill.jig.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.apache.drill.jig.api.DrillConnection;

public class JigJdbcConnection implements Connection
{
  private DrillConnection drillConn;
  private String currentSchema;
  
  // Jig allows just one concurrent query. Creating a new result set
  // implicitly closes the prior one.
  
  private JigJdbcStatement currentStatement;
  
  public JigJdbcConnection(DrillConnection conn, String schema) {
    drillConn = conn;
    currentSchema = schema;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    validateOpen( );
    if ( ! autoCommit ) {
      throw new UnsupportedException( "explicit commit" );
    }
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    // Drill does not support transactions, so this always
    // returns true.
    
    validateOpen( );
    return true;
  }

  @Override
  public void commit() throws SQLException {
    // Does nothing since Drill does not support transactions.
    // Just closes the current cursor.
    
    validateOpen( );
    if ( currentStatement != null )
      currentStatement.close( );
  }

  @Override
  public void rollback() throws SQLException {
    // Does nothing since Drill does not support transactions.
    // Just closes the current cursor.
    
    validateOpen( );
    if ( currentStatement != null )
      currentStatement.close( );
  }

  @Override
  public void close() throws SQLException {
    if ( drillConn != null ) {
      if ( currentStatement != null )
        currentStatement.close( );
      try {
        drillConn.close();
      } catch (Exception e) {
        // Ignore
      }
      drillConn = null;
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return drillConn == null;
  }
  
  private void validateOpen( ) throws SQLException {
    if ( drillConn == null )
      throw new SQLException( "Connection closed" );
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    validateOpen( );
    return new JigDatabaseMetaData( this );
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    validateOpen( );
    if ( ! readOnly ) {
      throw new UnsupportedException( "Drill is read-only" );
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    validateOpen( );
    // Drill is always read-only
    return true;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub

  }

  @Override
  public String getCatalog() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    validateOpen( );
    if ( level != Connection.TRANSACTION_NONE ) {
      throw new UnsupportedException( "Transaction isolation levels" );
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    validateOpen( );
    // Drill does not support transactions, so all reads are of
    // data currently on disk; essentially uncommitted reads.
    
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub

  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub

  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    validateOpen( );
    if ( holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT ) {
      throw new UnsupportedException( "Close cursors at commit" );
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    validateOpen( );
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "savepoints" );
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "savepoints" );
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "savepoints" );
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "savepoints" );
  }

  @Override
  public Statement createStatement() throws SQLException {
    validateOpen( );
    return new JigJdbcStatement( this );
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    validateOpen( );
    return createStatement( resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    validateOpen( );
    if ( resultSetType != ResultSet.TYPE_FORWARD_ONLY )
      throw new UnsupportedException( "Other than forward only result sets" );
    if ( resultSetConcurrency != ResultSet.CONCUR_READ_ONLY )
      throw new UnsupportedException( "Updatable result sets" );
    return new JigJdbcStatement( this );
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Prepared statements" );
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Prepared statements" );
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Prepared statements" );
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Prepared statements" );
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Prepared statements" );
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Prepared statements" );
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Procedures" );
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Procedures" );
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    validateOpen( );
    throw new UnsupportedException( "Procedures" );
  }

  @Override
  public Clob createClob() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return ! isClosed( );
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    // TODO Auto-generated method stub

  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub

  }

  @Override
  public String getSchema() throws SQLException {
    validateOpen( );
    return currentSchema;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    validateOpen( );
    close( );
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub

  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    validateOpen( );
    // TODO Auto-generated method stub
    return 0;
  }

  public DrillConnection getJigConnection() throws SQLException {
    validateOpen( );
    return drillConn;
  }
  
  protected void startResultSet( JigJdbcStatement stmt ) throws SQLException {
    validateOpen( );
    assert stmt != null;
    if ( currentStatement != null )
      currentStatement.close( );
    assert currentStatement == null;
    currentStatement = stmt;
  }
  
  protected void onStatementClose( JigJdbcStatement stmt ) {
    assert currentStatement == stmt;
    currentStatement = null;
  }

}

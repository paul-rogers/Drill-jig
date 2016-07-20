package org.apache.drill.jig.jdbc;

import java.sql.SQLFeatureNotSupportedException;

public class UnsupportedException extends SQLFeatureNotSupportedException
{
  public UnsupportedException(String feature) {
    super( "Not supported in Jig: " + feature );
  }

  private static final long serialVersionUID = 6216983856336588706L;

}

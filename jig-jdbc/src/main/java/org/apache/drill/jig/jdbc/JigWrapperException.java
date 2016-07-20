package org.apache.drill.jig.jdbc;

import java.sql.SQLException;

import org.apache.drill.jig.api.JigException;

public class JigWrapperException extends SQLException
{
  private static final long serialVersionUID = 4391308396194169465L;

  public JigWrapperException( JigException e ) {
    super( e.getMessage(), e );
  }
}

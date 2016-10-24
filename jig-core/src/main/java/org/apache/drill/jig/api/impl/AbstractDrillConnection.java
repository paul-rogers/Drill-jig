package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.DrillConnection;
import org.apache.drill.jig.exception.JigException;

public abstract class AbstractDrillConnection implements DrillConnection
{
  @Override
  public void alterSession( String key, int value ) throws JigException {
    doAlterSession( key, Integer.toString( value ) );
  }
  
  @Override
  public void alterSession( String key, String value ) throws JigException {
    doAlterSession( key, "'" + value + "'" );
  }
  
  private void doAlterSession( String key, String valueStr ) throws JigException {
    execute( String.format("alter session set `%s` = %s",
              key, valueStr) );
   }
  
}

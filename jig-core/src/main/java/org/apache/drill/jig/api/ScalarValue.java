package org.apache.drill.jig.api;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;

public interface ScalarValue
  {
    boolean getBoolean( );
    byte getByte( );
    short getShort( );
    int getInt( );
    long getLong( );
    float getFloat( );
    double getDouble( );
    BigDecimal getDecimal();
    String getString( );
    byte[] getBlob( );
    LocalDate getDate( );
    LocalDateTime getDateTime( );
    Period getUTCTime( );
    Object getValue( );
  }
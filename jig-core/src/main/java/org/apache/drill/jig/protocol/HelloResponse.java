package org.apache.drill.jig.protocol;

public class HelloResponse
{
  public int serverVersion;
  public int sessionVersion;
  
  public HelloResponse( int serverVers, int sessionVers ) {
    serverVersion = serverVers;
    sessionVersion = sessionVers;
  }
}
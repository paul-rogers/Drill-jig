package org.apache.drill.jig.protocol;

public class HelloRequest
{
  public int clientVersion;
  public int lowestClientVersion;
  
  public HelloRequest(int cVers, int lVers) {
    clientVersion = cVers;
    lowestClientVersion = lVers;
  }
}
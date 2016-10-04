package org.apache.drill.jig.extras.array;

public class Batch
{
  public String names[];
  public Object data[][];
  
  public Batch( String names[], Object data[][] ) {
    this.names = names;
    this.data = data;
  }
}
package org.apache.drill.jig.serde.serializer;

import org.apache.drill.jig.api.FieldValue;

public class FieldSerdeRegistry
{
  public interface FieldSerializer
  {
    void serialize( TupleWriter writer, FieldValue field );
  }
  
  public static class SerializeString implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeString( field.getString() );
    }   
  }
  
  public static class SerializeBoolean implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeBoolean( field.getBoolean() );
    }   
  }
  
  public static class SerializeLong implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeLong( field.getLong() );
    }   
  }
  
  public static class SerializeDouble implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeDouble( field.getDouble() );
    }   
  }
  
  public static class SerializeBigDecimal implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      writer.writeDecimal( field.getDecimal() );
    }   
  }
  
  public static class SerializeAny implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldValue field) {
      throw new IllegalStateException ( "Not yet" );
    }   
  }
  

}

package org.apache.drill.jig.serde;

import org.apache.drill.jig.api.FieldAccessor;

public class FieldSerdeRegistry
{
  public interface FieldSerializer
  {
    void serialize( TupleWriter writer, FieldAccessor field );
  }
  
  public static class SerializeString implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeString( field.asScalar().getString() );
    }   
  }
  
  public static class SerializeBoolean implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeBoolean( field.asScalar().getBoolean() );
    }   
  }
  
  public static class SerializeLong implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeLong( field.asScalar().getLong() );
    }   
  }
  
  public static class SerializeDouble implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeDouble( field.asScalar().getDouble() );
    }   
  }
  
  public static class SerializeBigDecimal implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      writer.writeDecimal( field.asScalar().getDecimal() );
    }   
  }
  
  public static class SerializeAny implements FieldSerializer
  {
    @Override
    public void serialize(TupleWriter writer, FieldAccessor field) {
      throw new IllegalStateException ( "Not yet" );
    }   
  }
  

}

package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.TupleSchema;

/**
 * Implementation for the schema of a field defined as a
 * {@link DataType#TUPLE}.
 */

public class TupleFieldSchemaImpl extends FieldSchemaImpl {

  private TupleSchema schema;

  public TupleFieldSchemaImpl(String name, boolean isNullable, TupleSchema schema) {
    super(name, DataType.TUPLE, isNullable);
    this.schema = schema;
  }

  @Override
  public TupleSchema schema() {
    return schema;
  }

  @Override
  protected void buildString(StringBuilder buf) {
    super.buildString( buf );
    buf.append( ", schema=" );
    buf.append( schema.toString() );
  }
}

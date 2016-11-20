package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;

/**
 * Implementation for fields of type {@link DataType#LIST}.
 */

public class ArrayFieldSchemaImpl extends FieldSchemaImpl {

  FieldSchema elementSchema;

  public ArrayFieldSchemaImpl(String name, boolean isNullable,
      FieldSchema elementSchema) {
    super(name, DataType.LIST, isNullable);
    this.elementSchema = elementSchema;
  }

  @Override
  public FieldSchema element() {
    return elementSchema;
  }

  @Override
  protected void buildString(StringBuilder buf) {
    super.buildString( buf );
    buf.append( ", element=" );
    buf.append( elementSchema.toString() );
  }
}

package org.apache.drill.jig.api.impl;

import org.apache.drill.jig.api.DataType;
import org.apache.drill.jig.api.FieldSchema;

public class ArrayFieldSchemaImpl extends FieldSchemaImpl {

	FieldSchema memberSchema;
	
	public ArrayFieldSchemaImpl(String name, boolean isNullable, FieldSchema memberSchema ) {
		super(name, DataType.LIST, isNullable);
		this.memberSchema = memberSchema;
	}
	
	public FieldSchema member( ) {
		return memberSchema;
	}
}

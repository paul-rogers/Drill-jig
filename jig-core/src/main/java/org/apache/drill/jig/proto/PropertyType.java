// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.jig.proto;

public enum PropertyType implements com.dyuproject.protostuff.EnumLite<PropertyType>
{
    STRING(1),
    INT(2),
    BOOLEAN(3);
    
    public final int number;
    
    private PropertyType (int number)
    {
        this.number = number;
    }
    
    public int getNumber()
    {
        return number;
    }
    
    public static PropertyType valueOf(int number)
    {
        switch(number) 
        {
            case 1: return STRING;
            case 2: return INT;
            case 3: return BOOLEAN;
            default: return null;
        }
    }
}

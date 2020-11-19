/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.vesoft.nebula.meta;

import org.apache.commons.lang.builder.HashCodeBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.BitSet;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.thrift.*;
import com.facebook.thrift.async.*;
import com.facebook.thrift.meta_data.*;
import com.facebook.thrift.server.*;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.protocol.*;

@SuppressWarnings({ "unused", "serial" })
public class DropHostFromZoneReq implements TBase, java.io.Serializable, Cloneable, Comparable<DropHostFromZoneReq> {
  private static final TStruct STRUCT_DESC = new TStruct("DropHostFromZoneReq");
  private static final TField NODE_FIELD_DESC = new TField("node", TType.STRUCT, (short)1);
  private static final TField ZONE_NAME_FIELD_DESC = new TField("zone_name", TType.STRING, (short)2);

  public com.vesoft.nebula.HostAddr node;
  public byte[] zone_name;
  public static final int NODE = 1;
  public static final int ZONE_NAME = 2;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(NODE, new FieldMetaData("node", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, com.vesoft.nebula.HostAddr.class)));
    tmpMetaDataMap.put(ZONE_NAME, new FieldMetaData("zone_name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(DropHostFromZoneReq.class, metaDataMap);
  }

  public DropHostFromZoneReq() {
  }

  public DropHostFromZoneReq(
    com.vesoft.nebula.HostAddr node,
    byte[] zone_name)
  {
    this();
    this.node = node;
    this.zone_name = zone_name;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public DropHostFromZoneReq(DropHostFromZoneReq other) {
    if (other.isSetNode()) {
      this.node = TBaseHelper.deepCopy(other.node);
    }
    if (other.isSetZone_name()) {
      this.zone_name = TBaseHelper.deepCopy(other.zone_name);
    }
  }

  public DropHostFromZoneReq deepCopy() {
    return new DropHostFromZoneReq(this);
  }

  @Deprecated
  public DropHostFromZoneReq clone() {
    return new DropHostFromZoneReq(this);
  }

  public com.vesoft.nebula.HostAddr  getNode() {
    return this.node;
  }

  public DropHostFromZoneReq setNode(com.vesoft.nebula.HostAddr node) {
    this.node = node;
    return this;
  }

  public void unsetNode() {
    this.node = null;
  }

  // Returns true if field node is set (has been assigned a value) and false otherwise
  public boolean isSetNode() {
    return this.node != null;
  }

  public void setNodeIsSet(boolean value) {
    if (!value) {
      this.node = null;
    }
  }

  public byte[]  getZone_name() {
    return this.zone_name;
  }

  public DropHostFromZoneReq setZone_name(byte[] zone_name) {
    this.zone_name = zone_name;
    return this;
  }

  public void unsetZone_name() {
    this.zone_name = null;
  }

  // Returns true if field zone_name is set (has been assigned a value) and false otherwise
  public boolean isSetZone_name() {
    return this.zone_name != null;
  }

  public void setZone_nameIsSet(boolean value) {
    if (!value) {
      this.zone_name = null;
    }
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case NODE:
      if (value == null) {
        unsetNode();
      } else {
        setNode((com.vesoft.nebula.HostAddr)value);
      }
      break;

    case ZONE_NAME:
      if (value == null) {
        unsetZone_name();
      } else {
        setZone_name((byte[])value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case NODE:
      return getNode();

    case ZONE_NAME:
      return getZone_name();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case NODE:
      return isSetNode();
    case ZONE_NAME:
      return isSetZone_name();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof DropHostFromZoneReq)
      return this.equals((DropHostFromZoneReq)that);
    return false;
  }

  public boolean equals(DropHostFromZoneReq that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_node = true && this.isSetNode();
    boolean that_present_node = true && that.isSetNode();
    if (this_present_node || that_present_node) {
      if (!(this_present_node && that_present_node))
        return false;
      if (!TBaseHelper.equalsNobinary(this.node, that.node))
        return false;
    }

    boolean this_present_zone_name = true && this.isSetZone_name();
    boolean that_present_zone_name = true && that.isSetZone_name();
    if (this_present_zone_name || that_present_zone_name) {
      if (!(this_present_zone_name && that_present_zone_name))
        return false;
      if (!TBaseHelper.equalsSlow(this.zone_name, that.zone_name))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_node = true && (isSetNode());
    builder.append(present_node);
    if (present_node)
      builder.append(node);

    boolean present_zone_name = true && (isSetZone_name());
    builder.append(present_zone_name);
    if (present_zone_name)
      builder.append(zone_name);

    return builder.toHashCode();
  }

  @Override
  public int compareTo(DropHostFromZoneReq other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetNode()).compareTo(other.isSetNode());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(node, other.node);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetZone_name()).compareTo(other.isSetZone_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(zone_name, other.zone_name);
    if (lastComparison != 0) {
      return lastComparison;
    }
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin(metaDataMap);
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case NODE:
          if (field.type == TType.STRUCT) {
            this.node = new com.vesoft.nebula.HostAddr();
            this.node.read(iprot);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case ZONE_NAME:
          if (field.type == TType.STRING) {
            this.zone_name = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();


    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.node != null) {
      oprot.writeFieldBegin(NODE_FIELD_DESC);
      this.node.write(oprot);
      oprot.writeFieldEnd();
    }
    if (this.zone_name != null) {
      oprot.writeFieldBegin(ZONE_NAME_FIELD_DESC);
      oprot.writeBinary(this.zone_name);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    return toString(DEFAULT_PRETTY_PRINT);
  }

  @Override
  public String toString(boolean prettyPrint) {
    return toString(1, prettyPrint);
  }

  @Override
  public String toString(int indent, boolean prettyPrint) {
    String indentStr = prettyPrint ? TBaseHelper.getIndentedString(indent) : "";
    String newLine = prettyPrint ? "\n" : "";
String space = prettyPrint ? " " : "";
    StringBuilder sb = new StringBuilder("DropHostFromZoneReq");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("node");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getNode() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getNode(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("zone_name");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getZone_name() == null) {
      sb.append("null");
    } else {
        int __zone_name_size = Math.min(this. getZone_name().length, 128);
        for (int i = 0; i < __zone_name_size; i++) {
          if (i != 0) sb.append(" ");
          sb.append(Integer.toHexString(this. getZone_name()[i]).length() > 1 ? Integer.toHexString(this. getZone_name()[i]).substring(Integer.toHexString(this. getZone_name()[i]).length() - 2).toUpperCase() : "0" + Integer.toHexString(this. getZone_name()[i]).toUpperCase());
        }
        if (this. getZone_name().length > 128) sb.append(" ...");
    }
    first = false;
    sb.append(newLine + TBaseHelper.reduceIndent(indentStr));
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}


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
public class DrainZoneReq implements TBase, java.io.Serializable, Cloneable, Comparable<DrainZoneReq> {
  private static final TStruct STRUCT_DESC = new TStruct("DrainZoneReq");
  private static final TField GROUP_NAME_FIELD_DESC = new TField("group_name", TType.STRING, (short)1);
  private static final TField ZONE_NAME_FIELD_DESC = new TField("zone_name", TType.STRING, (short)2);

  public byte[] group_name;
  public byte[] zone_name;
  public static final int GROUP_NAME = 1;
  public static final int ZONE_NAME = 2;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(GROUP_NAME, new FieldMetaData("group_name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    tmpMetaDataMap.put(ZONE_NAME, new FieldMetaData("zone_name", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(DrainZoneReq.class, metaDataMap);
  }

  public DrainZoneReq() {
  }

  public DrainZoneReq(
    byte[] group_name,
    byte[] zone_name)
  {
    this();
    this.group_name = group_name;
    this.zone_name = zone_name;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public DrainZoneReq(DrainZoneReq other) {
    if (other.isSetGroup_name()) {
      this.group_name = TBaseHelper.deepCopy(other.group_name);
    }
    if (other.isSetZone_name()) {
      this.zone_name = TBaseHelper.deepCopy(other.zone_name);
    }
  }

  public DrainZoneReq deepCopy() {
    return new DrainZoneReq(this);
  }

  @Deprecated
  public DrainZoneReq clone() {
    return new DrainZoneReq(this);
  }

  public byte[]  getGroup_name() {
    return this.group_name;
  }

  public DrainZoneReq setGroup_name(byte[] group_name) {
    this.group_name = group_name;
    return this;
  }

  public void unsetGroup_name() {
    this.group_name = null;
  }

  // Returns true if field group_name is set (has been assigned a value) and false otherwise
  public boolean isSetGroup_name() {
    return this.group_name != null;
  }

  public void setGroup_nameIsSet(boolean value) {
    if (!value) {
      this.group_name = null;
    }
  }

  public byte[]  getZone_name() {
    return this.zone_name;
  }

  public DrainZoneReq setZone_name(byte[] zone_name) {
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
    case GROUP_NAME:
      if (value == null) {
        unsetGroup_name();
      } else {
        setGroup_name((byte[])value);
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
    case GROUP_NAME:
      return getGroup_name();

    case ZONE_NAME:
      return getZone_name();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case GROUP_NAME:
      return isSetGroup_name();
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
    if (that instanceof DrainZoneReq)
      return this.equals((DrainZoneReq)that);
    return false;
  }

  public boolean equals(DrainZoneReq that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_group_name = true && this.isSetGroup_name();
    boolean that_present_group_name = true && that.isSetGroup_name();
    if (this_present_group_name || that_present_group_name) {
      if (!(this_present_group_name && that_present_group_name))
        return false;
      if (!TBaseHelper.equalsSlow(this.group_name, that.group_name))
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

    boolean present_group_name = true && (isSetGroup_name());
    builder.append(present_group_name);
    if (present_group_name)
      builder.append(group_name);

    boolean present_zone_name = true && (isSetZone_name());
    builder.append(present_zone_name);
    if (present_zone_name)
      builder.append(zone_name);

    return builder.toHashCode();
  }

  @Override
  public int compareTo(DrainZoneReq other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetGroup_name()).compareTo(other.isSetGroup_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(group_name, other.group_name);
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
        case GROUP_NAME:
          if (field.type == TType.STRING) {
            this.group_name = iprot.readBinary();
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
    if (this.group_name != null) {
      oprot.writeFieldBegin(GROUP_NAME_FIELD_DESC);
      oprot.writeBinary(this.group_name);
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
    StringBuilder sb = new StringBuilder("DrainZoneReq");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("group_name");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getGroup_name() == null) {
      sb.append("null");
    } else {
        int __group_name_size = Math.min(this. getGroup_name().length, 128);
        for (int i = 0; i < __group_name_size; i++) {
          if (i != 0) sb.append(" ");
          sb.append(Integer.toHexString(this. getGroup_name()[i]).length() > 1 ? Integer.toHexString(this. getGroup_name()[i]).substring(Integer.toHexString(this. getGroup_name()[i]).length() - 2).toUpperCase() : "0" + Integer.toHexString(this. getGroup_name()[i]).toUpperCase());
        }
        if (this. getGroup_name().length > 128) sb.append(" ...");
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


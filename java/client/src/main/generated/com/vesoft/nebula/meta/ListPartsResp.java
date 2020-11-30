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
public class ListPartsResp implements TBase, java.io.Serializable, Cloneable, Comparable<ListPartsResp> {
  private static final TStruct STRUCT_DESC = new TStruct("ListPartsResp");
  private static final TField CODE_FIELD_DESC = new TField("code", TType.I32, (short)1);
  private static final TField LEADER_FIELD_DESC = new TField("leader", TType.STRUCT, (short)2);
  private static final TField PARTS_FIELD_DESC = new TField("parts", TType.LIST, (short)3);

  /**
   * 
   * @see ErrorCode
   */
  public int code;
  public com.vesoft.nebula.HostAddr leader;
  public List<PartItem> parts;
  public static final int CODE = 1;
  public static final int LEADER = 2;
  public static final int PARTS = 3;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments
  private static final int __CODE_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(CODE, new FieldMetaData("code", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    tmpMetaDataMap.put(LEADER, new FieldMetaData("leader", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, com.vesoft.nebula.HostAddr.class)));
    tmpMetaDataMap.put(PARTS, new FieldMetaData("parts", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new StructMetaData(TType.STRUCT, PartItem.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(ListPartsResp.class, metaDataMap);
  }

  public ListPartsResp() {
  }

  public ListPartsResp(
    int code,
    com.vesoft.nebula.HostAddr leader,
    List<PartItem> parts)
  {
    this();
    this.code = code;
    setCodeIsSet(true);
    this.leader = leader;
    this.parts = parts;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public ListPartsResp(ListPartsResp other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.code = TBaseHelper.deepCopy(other.code);
    if (other.isSetLeader()) {
      this.leader = TBaseHelper.deepCopy(other.leader);
    }
    if (other.isSetParts()) {
      this.parts = TBaseHelper.deepCopy(other.parts);
    }
  }

  public ListPartsResp deepCopy() {
    return new ListPartsResp(this);
  }

  @Deprecated
  public ListPartsResp clone() {
    return new ListPartsResp(this);
  }

  /**
   * 
   * @see ErrorCode
   */
  public int  getCode() {
    return this.code;
  }

  /**
   * 
   * @see ErrorCode
   */
  public ListPartsResp setCode(int code) {
    this.code = code;
    setCodeIsSet(true);
    return this;
  }

  public void unsetCode() {
    __isset_bit_vector.clear(__CODE_ISSET_ID);
  }

  // Returns true if field code is set (has been assigned a value) and false otherwise
  public boolean isSetCode() {
    return __isset_bit_vector.get(__CODE_ISSET_ID);
  }

  public void setCodeIsSet(boolean value) {
    __isset_bit_vector.set(__CODE_ISSET_ID, value);
  }

  public com.vesoft.nebula.HostAddr  getLeader() {
    return this.leader;
  }

  public ListPartsResp setLeader(com.vesoft.nebula.HostAddr leader) {
    this.leader = leader;
    return this;
  }

  public void unsetLeader() {
    this.leader = null;
  }

  // Returns true if field leader is set (has been assigned a value) and false otherwise
  public boolean isSetLeader() {
    return this.leader != null;
  }

  public void setLeaderIsSet(boolean value) {
    if (!value) {
      this.leader = null;
    }
  }

  public List<PartItem>  getParts() {
    return this.parts;
  }

  public ListPartsResp setParts(List<PartItem> parts) {
    this.parts = parts;
    return this;
  }

  public void unsetParts() {
    this.parts = null;
  }

  // Returns true if field parts is set (has been assigned a value) and false otherwise
  public boolean isSetParts() {
    return this.parts != null;
  }

  public void setPartsIsSet(boolean value) {
    if (!value) {
      this.parts = null;
    }
  }

  @SuppressWarnings("unchecked")
  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case CODE:
      if (value == null) {
        unsetCode();
      } else {
        setCode((Integer)value);
      }
      break;

    case LEADER:
      if (value == null) {
        unsetLeader();
      } else {
        setLeader((com.vesoft.nebula.HostAddr)value);
      }
      break;

    case PARTS:
      if (value == null) {
        unsetParts();
      } else {
        setParts((List<PartItem>)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case CODE:
      return getCode();

    case LEADER:
      return getLeader();

    case PARTS:
      return getParts();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case CODE:
      return isSetCode();
    case LEADER:
      return isSetLeader();
    case PARTS:
      return isSetParts();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof ListPartsResp)
      return this.equals((ListPartsResp)that);
    return false;
  }

  public boolean equals(ListPartsResp that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_code = true;
    boolean that_present_code = true;
    if (this_present_code || that_present_code) {
      if (!(this_present_code && that_present_code))
        return false;
      if (!TBaseHelper.equalsNobinary(this.code, that.code))
        return false;
    }

    boolean this_present_leader = true && this.isSetLeader();
    boolean that_present_leader = true && that.isSetLeader();
    if (this_present_leader || that_present_leader) {
      if (!(this_present_leader && that_present_leader))
        return false;
      if (!TBaseHelper.equalsNobinary(this.leader, that.leader))
        return false;
    }

    boolean this_present_parts = true && this.isSetParts();
    boolean that_present_parts = true && that.isSetParts();
    if (this_present_parts || that_present_parts) {
      if (!(this_present_parts && that_present_parts))
        return false;
      if (!TBaseHelper.equalsNobinary(this.parts, that.parts))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_code = true;
    builder.append(present_code);
    if (present_code)
      builder.append(code);

    boolean present_leader = true && (isSetLeader());
    builder.append(present_leader);
    if (present_leader)
      builder.append(leader);

    boolean present_parts = true && (isSetParts());
    builder.append(present_parts);
    if (present_parts)
      builder.append(parts);

    return builder.toHashCode();
  }

  @Override
  public int compareTo(ListPartsResp other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetCode()).compareTo(other.isSetCode());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(code, other.code);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetLeader()).compareTo(other.isSetLeader());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(leader, other.leader);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetParts()).compareTo(other.isSetParts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(parts, other.parts);
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
        case CODE:
          if (field.type == TType.I32) {
            this.code = iprot.readI32();
            setCodeIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case LEADER:
          if (field.type == TType.STRUCT) {
            this.leader = new com.vesoft.nebula.HostAddr();
            this.leader.read(iprot);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case PARTS:
          if (field.type == TType.LIST) {
            {
              TList _list88 = iprot.readListBegin();
              this.parts = new ArrayList<PartItem>(Math.max(0, _list88.size));
              for (int _i89 = 0; 
                   (_list88.size < 0) ? iprot.peekList() : (_i89 < _list88.size); 
                   ++_i89)
              {
                PartItem _elem90;
                _elem90 = new PartItem();
                _elem90.read(iprot);
                this.parts.add(_elem90);
              }
              iprot.readListEnd();
            }
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
    oprot.writeFieldBegin(CODE_FIELD_DESC);
    oprot.writeI32(this.code);
    oprot.writeFieldEnd();
    if (this.leader != null) {
      oprot.writeFieldBegin(LEADER_FIELD_DESC);
      this.leader.write(oprot);
      oprot.writeFieldEnd();
    }
    if (this.parts != null) {
      oprot.writeFieldBegin(PARTS_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRUCT, this.parts.size()));
        for (PartItem _iter91 : this.parts)        {
          _iter91.write(oprot);
        }
        oprot.writeListEnd();
      }
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
    StringBuilder sb = new StringBuilder("ListPartsResp");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("code");
    sb.append(space);
    sb.append(":").append(space);
    String code_name = ErrorCode.VALUES_TO_NAMES.get(this. getCode());
    if (code_name != null) {
      sb.append(code_name);
      sb.append(" (");
    }
    sb.append(this. getCode());
    if (code_name != null) {
      sb.append(")");
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("leader");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getLeader() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getLeader(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("parts");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getParts() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getParts(), indent + 1, prettyPrint));
    }
    first = false;
    sb.append(newLine + TBaseHelper.reduceIndent(indentStr));
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
    if (isSetCode() && !ErrorCode.VALID_VALUES.contains(code)){
      throw new TProtocolException("The field 'code' has been assigned the invalid value " + code);
    }
  }

}


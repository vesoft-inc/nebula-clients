/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.vesoft.nebula.raftex;

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
public class AppendLogResponse implements TBase, java.io.Serializable, Cloneable, Comparable<AppendLogResponse> {
  private static final TStruct STRUCT_DESC = new TStruct("AppendLogResponse");
  private static final TField ERROR_CODE_FIELD_DESC = new TField("error_code", TType.I32, (short)1);
  private static final TField CURRENT_TERM_FIELD_DESC = new TField("current_term", TType.I64, (short)2);
  private static final TField LEADER_ADDR_FIELD_DESC = new TField("leader_addr", TType.STRING, (short)3);
  private static final TField LEADER_PORT_FIELD_DESC = new TField("leader_port", TType.I32, (short)4);
  private static final TField COMMITTED_LOG_ID_FIELD_DESC = new TField("committed_log_id", TType.I64, (short)5);
  private static final TField LAST_LOG_ID_FIELD_DESC = new TField("last_log_id", TType.I64, (short)6);
  private static final TField LAST_LOG_TERM_FIELD_DESC = new TField("last_log_term", TType.I64, (short)7);

  /**
   * 
   * @see ErrorCode
   */
  public int error_code;
  public long current_term;
  public String leader_addr;
  public int leader_port;
  public long committed_log_id;
  public long last_log_id;
  public long last_log_term;
  public static final int ERROR_CODE = 1;
  public static final int CURRENT_TERM = 2;
  public static final int LEADER_ADDR = 3;
  public static final int LEADER_PORT = 4;
  public static final int COMMITTED_LOG_ID = 5;
  public static final int LAST_LOG_ID = 6;
  public static final int LAST_LOG_TERM = 7;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments
  private static final int __ERROR_CODE_ISSET_ID = 0;
  private static final int __CURRENT_TERM_ISSET_ID = 1;
  private static final int __LEADER_PORT_ISSET_ID = 2;
  private static final int __COMMITTED_LOG_ID_ISSET_ID = 3;
  private static final int __LAST_LOG_ID_ISSET_ID = 4;
  private static final int __LAST_LOG_TERM_ISSET_ID = 5;
  private BitSet __isset_bit_vector = new BitSet(6);

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(ERROR_CODE, new FieldMetaData("error_code", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    tmpMetaDataMap.put(CURRENT_TERM, new FieldMetaData("current_term", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    tmpMetaDataMap.put(LEADER_ADDR, new FieldMetaData("leader_addr", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    tmpMetaDataMap.put(LEADER_PORT, new FieldMetaData("leader_port", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    tmpMetaDataMap.put(COMMITTED_LOG_ID, new FieldMetaData("committed_log_id", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    tmpMetaDataMap.put(LAST_LOG_ID, new FieldMetaData("last_log_id", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    tmpMetaDataMap.put(LAST_LOG_TERM, new FieldMetaData("last_log_term", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(AppendLogResponse.class, metaDataMap);
  }

  public AppendLogResponse() {
  }

  public AppendLogResponse(
    int error_code,
    long current_term,
    String leader_addr,
    int leader_port,
    long committed_log_id,
    long last_log_id,
    long last_log_term)
  {
    this();
    this.error_code = error_code;
    setError_codeIsSet(true);
    this.current_term = current_term;
    setCurrent_termIsSet(true);
    this.leader_addr = leader_addr;
    this.leader_port = leader_port;
    setLeader_portIsSet(true);
    this.committed_log_id = committed_log_id;
    setCommitted_log_idIsSet(true);
    this.last_log_id = last_log_id;
    setLast_log_idIsSet(true);
    this.last_log_term = last_log_term;
    setLast_log_termIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public AppendLogResponse(AppendLogResponse other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.error_code = TBaseHelper.deepCopy(other.error_code);
    this.current_term = TBaseHelper.deepCopy(other.current_term);
    if (other.isSetLeader_addr()) {
      this.leader_addr = TBaseHelper.deepCopy(other.leader_addr);
    }
    this.leader_port = TBaseHelper.deepCopy(other.leader_port);
    this.committed_log_id = TBaseHelper.deepCopy(other.committed_log_id);
    this.last_log_id = TBaseHelper.deepCopy(other.last_log_id);
    this.last_log_term = TBaseHelper.deepCopy(other.last_log_term);
  }

  public AppendLogResponse deepCopy() {
    return new AppendLogResponse(this);
  }

  @Deprecated
  public AppendLogResponse clone() {
    return new AppendLogResponse(this);
  }

  /**
   * 
   * @see ErrorCode
   */
  public int  getError_code() {
    return this.error_code;
  }

  /**
   * 
   * @see ErrorCode
   */
  public AppendLogResponse setError_code(int error_code) {
    this.error_code = error_code;
    setError_codeIsSet(true);
    return this;
  }

  public void unsetError_code() {
    __isset_bit_vector.clear(__ERROR_CODE_ISSET_ID);
  }

  // Returns true if field error_code is set (has been assigned a value) and false otherwise
  public boolean isSetError_code() {
    return __isset_bit_vector.get(__ERROR_CODE_ISSET_ID);
  }

  public void setError_codeIsSet(boolean value) {
    __isset_bit_vector.set(__ERROR_CODE_ISSET_ID, value);
  }

  public long  getCurrent_term() {
    return this.current_term;
  }

  public AppendLogResponse setCurrent_term(long current_term) {
    this.current_term = current_term;
    setCurrent_termIsSet(true);
    return this;
  }

  public void unsetCurrent_term() {
    __isset_bit_vector.clear(__CURRENT_TERM_ISSET_ID);
  }

  // Returns true if field current_term is set (has been assigned a value) and false otherwise
  public boolean isSetCurrent_term() {
    return __isset_bit_vector.get(__CURRENT_TERM_ISSET_ID);
  }

  public void setCurrent_termIsSet(boolean value) {
    __isset_bit_vector.set(__CURRENT_TERM_ISSET_ID, value);
  }

  public String  getLeader_addr() {
    return this.leader_addr;
  }

  public AppendLogResponse setLeader_addr(String leader_addr) {
    this.leader_addr = leader_addr;
    return this;
  }

  public void unsetLeader_addr() {
    this.leader_addr = null;
  }

  // Returns true if field leader_addr is set (has been assigned a value) and false otherwise
  public boolean isSetLeader_addr() {
    return this.leader_addr != null;
  }

  public void setLeader_addrIsSet(boolean value) {
    if (!value) {
      this.leader_addr = null;
    }
  }

  public int  getLeader_port() {
    return this.leader_port;
  }

  public AppendLogResponse setLeader_port(int leader_port) {
    this.leader_port = leader_port;
    setLeader_portIsSet(true);
    return this;
  }

  public void unsetLeader_port() {
    __isset_bit_vector.clear(__LEADER_PORT_ISSET_ID);
  }

  // Returns true if field leader_port is set (has been assigned a value) and false otherwise
  public boolean isSetLeader_port() {
    return __isset_bit_vector.get(__LEADER_PORT_ISSET_ID);
  }

  public void setLeader_portIsSet(boolean value) {
    __isset_bit_vector.set(__LEADER_PORT_ISSET_ID, value);
  }

  public long  getCommitted_log_id() {
    return this.committed_log_id;
  }

  public AppendLogResponse setCommitted_log_id(long committed_log_id) {
    this.committed_log_id = committed_log_id;
    setCommitted_log_idIsSet(true);
    return this;
  }

  public void unsetCommitted_log_id() {
    __isset_bit_vector.clear(__COMMITTED_LOG_ID_ISSET_ID);
  }

  // Returns true if field committed_log_id is set (has been assigned a value) and false otherwise
  public boolean isSetCommitted_log_id() {
    return __isset_bit_vector.get(__COMMITTED_LOG_ID_ISSET_ID);
  }

  public void setCommitted_log_idIsSet(boolean value) {
    __isset_bit_vector.set(__COMMITTED_LOG_ID_ISSET_ID, value);
  }

  public long  getLast_log_id() {
    return this.last_log_id;
  }

  public AppendLogResponse setLast_log_id(long last_log_id) {
    this.last_log_id = last_log_id;
    setLast_log_idIsSet(true);
    return this;
  }

  public void unsetLast_log_id() {
    __isset_bit_vector.clear(__LAST_LOG_ID_ISSET_ID);
  }

  // Returns true if field last_log_id is set (has been assigned a value) and false otherwise
  public boolean isSetLast_log_id() {
    return __isset_bit_vector.get(__LAST_LOG_ID_ISSET_ID);
  }

  public void setLast_log_idIsSet(boolean value) {
    __isset_bit_vector.set(__LAST_LOG_ID_ISSET_ID, value);
  }

  public long  getLast_log_term() {
    return this.last_log_term;
  }

  public AppendLogResponse setLast_log_term(long last_log_term) {
    this.last_log_term = last_log_term;
    setLast_log_termIsSet(true);
    return this;
  }

  public void unsetLast_log_term() {
    __isset_bit_vector.clear(__LAST_LOG_TERM_ISSET_ID);
  }

  // Returns true if field last_log_term is set (has been assigned a value) and false otherwise
  public boolean isSetLast_log_term() {
    return __isset_bit_vector.get(__LAST_LOG_TERM_ISSET_ID);
  }

  public void setLast_log_termIsSet(boolean value) {
    __isset_bit_vector.set(__LAST_LOG_TERM_ISSET_ID, value);
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case ERROR_CODE:
      if (value == null) {
        unsetError_code();
      } else {
        setError_code((Integer)value);
      }
      break;

    case CURRENT_TERM:
      if (value == null) {
        unsetCurrent_term();
      } else {
        setCurrent_term((Long)value);
      }
      break;

    case LEADER_ADDR:
      if (value == null) {
        unsetLeader_addr();
      } else {
        setLeader_addr((String)value);
      }
      break;

    case LEADER_PORT:
      if (value == null) {
        unsetLeader_port();
      } else {
        setLeader_port((Integer)value);
      }
      break;

    case COMMITTED_LOG_ID:
      if (value == null) {
        unsetCommitted_log_id();
      } else {
        setCommitted_log_id((Long)value);
      }
      break;

    case LAST_LOG_ID:
      if (value == null) {
        unsetLast_log_id();
      } else {
        setLast_log_id((Long)value);
      }
      break;

    case LAST_LOG_TERM:
      if (value == null) {
        unsetLast_log_term();
      } else {
        setLast_log_term((Long)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case ERROR_CODE:
      return getError_code();

    case CURRENT_TERM:
      return new Long(getCurrent_term());

    case LEADER_ADDR:
      return getLeader_addr();

    case LEADER_PORT:
      return new Integer(getLeader_port());

    case COMMITTED_LOG_ID:
      return new Long(getCommitted_log_id());

    case LAST_LOG_ID:
      return new Long(getLast_log_id());

    case LAST_LOG_TERM:
      return new Long(getLast_log_term());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case ERROR_CODE:
      return isSetError_code();
    case CURRENT_TERM:
      return isSetCurrent_term();
    case LEADER_ADDR:
      return isSetLeader_addr();
    case LEADER_PORT:
      return isSetLeader_port();
    case COMMITTED_LOG_ID:
      return isSetCommitted_log_id();
    case LAST_LOG_ID:
      return isSetLast_log_id();
    case LAST_LOG_TERM:
      return isSetLast_log_term();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof AppendLogResponse)
      return this.equals((AppendLogResponse)that);
    return false;
  }

  public boolean equals(AppendLogResponse that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_error_code = true;
    boolean that_present_error_code = true;
    if (this_present_error_code || that_present_error_code) {
      if (!(this_present_error_code && that_present_error_code))
        return false;
      if (!TBaseHelper.equalsNobinary(this.error_code, that.error_code))
        return false;
    }

    boolean this_present_current_term = true;
    boolean that_present_current_term = true;
    if (this_present_current_term || that_present_current_term) {
      if (!(this_present_current_term && that_present_current_term))
        return false;
      if (!TBaseHelper.equalsNobinary(this.current_term, that.current_term))
        return false;
    }

    boolean this_present_leader_addr = true && this.isSetLeader_addr();
    boolean that_present_leader_addr = true && that.isSetLeader_addr();
    if (this_present_leader_addr || that_present_leader_addr) {
      if (!(this_present_leader_addr && that_present_leader_addr))
        return false;
      if (!TBaseHelper.equalsNobinary(this.leader_addr, that.leader_addr))
        return false;
    }

    boolean this_present_leader_port = true;
    boolean that_present_leader_port = true;
    if (this_present_leader_port || that_present_leader_port) {
      if (!(this_present_leader_port && that_present_leader_port))
        return false;
      if (!TBaseHelper.equalsNobinary(this.leader_port, that.leader_port))
        return false;
    }

    boolean this_present_committed_log_id = true;
    boolean that_present_committed_log_id = true;
    if (this_present_committed_log_id || that_present_committed_log_id) {
      if (!(this_present_committed_log_id && that_present_committed_log_id))
        return false;
      if (!TBaseHelper.equalsNobinary(this.committed_log_id, that.committed_log_id))
        return false;
    }

    boolean this_present_last_log_id = true;
    boolean that_present_last_log_id = true;
    if (this_present_last_log_id || that_present_last_log_id) {
      if (!(this_present_last_log_id && that_present_last_log_id))
        return false;
      if (!TBaseHelper.equalsNobinary(this.last_log_id, that.last_log_id))
        return false;
    }

    boolean this_present_last_log_term = true;
    boolean that_present_last_log_term = true;
    if (this_present_last_log_term || that_present_last_log_term) {
      if (!(this_present_last_log_term && that_present_last_log_term))
        return false;
      if (!TBaseHelper.equalsNobinary(this.last_log_term, that.last_log_term))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_error_code = true;
    builder.append(present_error_code);
    if (present_error_code)
      builder.append(error_code);

    boolean present_current_term = true;
    builder.append(present_current_term);
    if (present_current_term)
      builder.append(current_term);

    boolean present_leader_addr = true && (isSetLeader_addr());
    builder.append(present_leader_addr);
    if (present_leader_addr)
      builder.append(leader_addr);

    boolean present_leader_port = true;
    builder.append(present_leader_port);
    if (present_leader_port)
      builder.append(leader_port);

    boolean present_committed_log_id = true;
    builder.append(present_committed_log_id);
    if (present_committed_log_id)
      builder.append(committed_log_id);

    boolean present_last_log_id = true;
    builder.append(present_last_log_id);
    if (present_last_log_id)
      builder.append(last_log_id);

    boolean present_last_log_term = true;
    builder.append(present_last_log_term);
    if (present_last_log_term)
      builder.append(last_log_term);

    return builder.toHashCode();
  }

  @Override
  public int compareTo(AppendLogResponse other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetError_code()).compareTo(other.isSetError_code());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(error_code, other.error_code);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetCurrent_term()).compareTo(other.isSetCurrent_term());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(current_term, other.current_term);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetLeader_addr()).compareTo(other.isSetLeader_addr());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(leader_addr, other.leader_addr);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetLeader_port()).compareTo(other.isSetLeader_port());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(leader_port, other.leader_port);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetCommitted_log_id()).compareTo(other.isSetCommitted_log_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(committed_log_id, other.committed_log_id);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetLast_log_id()).compareTo(other.isSetLast_log_id());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(last_log_id, other.last_log_id);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetLast_log_term()).compareTo(other.isSetLast_log_term());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(last_log_term, other.last_log_term);
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
        case ERROR_CODE:
          if (field.type == TType.I32) {
            this.error_code = iprot.readI32();
            setError_codeIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case CURRENT_TERM:
          if (field.type == TType.I64) {
            this.current_term = iprot.readI64();
            setCurrent_termIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case LEADER_ADDR:
          if (field.type == TType.STRING) {
            this.leader_addr = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case LEADER_PORT:
          if (field.type == TType.I32) {
            this.leader_port = iprot.readI32();
            setLeader_portIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case COMMITTED_LOG_ID:
          if (field.type == TType.I64) {
            this.committed_log_id = iprot.readI64();
            setCommitted_log_idIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case LAST_LOG_ID:
          if (field.type == TType.I64) {
            this.last_log_id = iprot.readI64();
            setLast_log_idIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case LAST_LOG_TERM:
          if (field.type == TType.I64) {
            this.last_log_term = iprot.readI64();
            setLast_log_termIsSet(true);
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
    oprot.writeFieldBegin(ERROR_CODE_FIELD_DESC);
    oprot.writeI32(this.error_code);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(CURRENT_TERM_FIELD_DESC);
    oprot.writeI64(this.current_term);
    oprot.writeFieldEnd();
    if (this.leader_addr != null) {
      oprot.writeFieldBegin(LEADER_ADDR_FIELD_DESC);
      oprot.writeString(this.leader_addr);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(LEADER_PORT_FIELD_DESC);
    oprot.writeI32(this.leader_port);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(COMMITTED_LOG_ID_FIELD_DESC);
    oprot.writeI64(this.committed_log_id);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(LAST_LOG_ID_FIELD_DESC);
    oprot.writeI64(this.last_log_id);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(LAST_LOG_TERM_FIELD_DESC);
    oprot.writeI64(this.last_log_term);
    oprot.writeFieldEnd();
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
    StringBuilder sb = new StringBuilder("AppendLogResponse");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("error_code");
    sb.append(space);
    sb.append(":").append(space);
    String error_code_name = ErrorCode.VALUES_TO_NAMES.get(this. getError_code());
    if (error_code_name != null) {
      sb.append(error_code_name);
      sb.append(" (");
    }
    sb.append(this. getError_code());
    if (error_code_name != null) {
      sb.append(")");
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("current_term");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getCurrent_term(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("leader_addr");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getLeader_addr() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getLeader_addr(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("leader_port");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getLeader_port(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("committed_log_id");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getCommitted_log_id(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("last_log_id");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getLast_log_id(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("last_log_term");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getLast_log_term(), indent + 1, prettyPrint));
    first = false;
    sb.append(newLine + TBaseHelper.reduceIndent(indentStr));
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
    if (isSetError_code() && !ErrorCode.VALID_VALUES.contains(error_code)){
      throw new TProtocolException("The field 'error_code' has been assigned the invalid value " + error_code);
    }
  }

}


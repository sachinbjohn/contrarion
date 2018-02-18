/**
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.cassandra.thrift;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import org.apache.commons.lang.builder.HashCodeBuilder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultigetCountResult implements org.apache.thrift.TBase<MultigetCountResult, MultigetCountResult._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("MultigetCountResult");

  private static final org.apache.thrift.protocol.TField VALUE_FIELD_DESC = new org.apache.thrift.protocol.TField("value", org.apache.thrift.protocol.TType.MAP, (short)1);
  private static final org.apache.thrift.protocol.TField LTS_FIELD_DESC = new org.apache.thrift.protocol.TField("lts", org.apache.thrift.protocol.TType.I64, (short)3);

  public Map<ByteBuffer,CountWithMetadata> value; // required
  public long lts; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    VALUE((short)1, "value"),
    LTS((short)3, "lts");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // VALUE
          return VALUE;
        case 3: // LTS
          return LTS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __LTS_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.VALUE, new org.apache.thrift.meta_data.FieldMetaData("value", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING            , true), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, CountWithMetadata.class))));
    tmpMap.put(_Fields.LTS, new org.apache.thrift.meta_data.FieldMetaData("lts", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64        , "LamportTimestamp")));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(MultigetCountResult.class, metaDataMap);
  }

  public MultigetCountResult() {
  }

  public MultigetCountResult(
    Map<ByteBuffer,CountWithMetadata> value,
    long lts)
  {
    this();
    this.value = value;
    this.lts = lts;
    setLtsIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public MultigetCountResult(MultigetCountResult other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetValue()) {
      Map<ByteBuffer,CountWithMetadata> __this__value = new HashMap<ByteBuffer,CountWithMetadata>();
      for (Map.Entry<ByteBuffer, CountWithMetadata> other_element : other.value.entrySet()) {

        ByteBuffer other_element_key = other_element.getKey();
        CountWithMetadata other_element_value = other_element.getValue();

        ByteBuffer __this__value_copy_key = org.apache.thrift.TBaseHelper.copyBinary(other_element_key);
;

        CountWithMetadata __this__value_copy_value = new CountWithMetadata(other_element_value);

        __this__value.put(__this__value_copy_key, __this__value_copy_value);
      }
      this.value = __this__value;
    }
    this.lts = other.lts;
  }

  public MultigetCountResult deepCopy() {
    return new MultigetCountResult(this);
  }

  @Override
  public void clear() {
    this.value = null;
    setLtsIsSet(false);
    this.lts = 0;
  }

  public int getValueSize() {
    return (this.value == null) ? 0 : this.value.size();
  }

  public void putToValue(ByteBuffer key, CountWithMetadata val) {
    if (this.value == null) {
      this.value = new HashMap<ByteBuffer,CountWithMetadata>();
    }
    this.value.put(key, val);
  }

  public Map<ByteBuffer,CountWithMetadata> getValue() {
    return this.value;
  }

  public MultigetCountResult setValue(Map<ByteBuffer,CountWithMetadata> value) {
    this.value = value;
    return this;
  }

  public void unsetValue() {
    this.value = null;
  }

  /** Returns true if field value is set (has been assigned a value) and false otherwise */
  public boolean isSetValue() {
    return this.value != null;
  }

  public void setValueIsSet(boolean value) {
    if (!value) {
      this.value = null;
    }
  }

  public long getLts() {
    return this.lts;
  }

  public MultigetCountResult setLts(long lts) {
    this.lts = lts;
    setLtsIsSet(true);
    return this;
  }

  public void unsetLts() {
    __isset_bit_vector.clear(__LTS_ISSET_ID);
  }

  /** Returns true if field lts is set (has been assigned a value) and false otherwise */
  public boolean isSetLts() {
    return __isset_bit_vector.get(__LTS_ISSET_ID);
  }

  public void setLtsIsSet(boolean value) {
    __isset_bit_vector.set(__LTS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case VALUE:
      if (value == null) {
        unsetValue();
      } else {
        setValue((Map<ByteBuffer,CountWithMetadata>)value);
      }
      break;

    case LTS:
      if (value == null) {
        unsetLts();
      } else {
        setLts((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case VALUE:
      return getValue();

    case LTS:
      return Long.valueOf(getLts());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case VALUE:
      return isSetValue();
    case LTS:
      return isSetLts();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof MultigetCountResult)
      return this.equals((MultigetCountResult)that);
    return false;
  }

  public boolean equals(MultigetCountResult that) {
    if (that == null)
      return false;

    boolean this_present_value = true && this.isSetValue();
    boolean that_present_value = true && that.isSetValue();
    if (this_present_value || that_present_value) {
      if (!(this_present_value && that_present_value))
        return false;
      if (!this.value.equals(that.value))
        return false;
    }

    boolean this_present_lts = true;
    boolean that_present_lts = true;
    if (this_present_lts || that_present_lts) {
      if (!(this_present_lts && that_present_lts))
        return false;
      if (this.lts != that.lts)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_value = true && (isSetValue());
    builder.append(present_value);
    if (present_value)
      builder.append(value);

    boolean present_lts = true;
    builder.append(present_lts);
    if (present_lts)
      builder.append(lts);

    return builder.toHashCode();
  }

  public int compareTo(MultigetCountResult other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    MultigetCountResult typedOther = (MultigetCountResult)other;

    lastComparison = Boolean.valueOf(isSetValue()).compareTo(typedOther.isSetValue());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetValue()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.value, typedOther.value);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLts()).compareTo(typedOther.isSetLts());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLts()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lts, typedOther.lts);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    org.apache.thrift.protocol.TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == org.apache.thrift.protocol.TType.STOP) { 
        break;
      }
      switch (field.id) {
        case 1: // VALUE
          if (field.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map121 = iprot.readMapBegin();
              this.value = new HashMap<ByteBuffer,CountWithMetadata>(2*_map121.size);
              for (int _i122 = 0; _i122 < _map121.size; ++_i122)
              {
                ByteBuffer _key123; // required
                CountWithMetadata _val124; // required
                _key123 = iprot.readBinary();
                _val124 = new CountWithMetadata();
                _val124.read(iprot);
                this.value.put(_key123, _val124);
              }
              iprot.readMapEnd();
            }
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // LTS
          if (field.type == org.apache.thrift.protocol.TType.I64) {
            this.lts = iprot.readI64();
            setLtsIsSet(true);
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.value != null) {
      oprot.writeFieldBegin(VALUE_FIELD_DESC);
      {
        oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRUCT, this.value.size()));
        for (Map.Entry<ByteBuffer, CountWithMetadata> _iter125 : this.value.entrySet())
        {
          oprot.writeBinary(_iter125.getKey());
          _iter125.getValue().write(oprot);
        }
        oprot.writeMapEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(LTS_FIELD_DESC);
    oprot.writeI64(this.lts);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("MultigetCountResult(");
    boolean first = true;

    sb.append("value:");
    if (this.value == null) {
      sb.append("null");
    } else {
      sb.append(this.value);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("lts:");
    sb.append(this.lts);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

}


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

/**
 * A named list of columns.
 * @param name. see Column.name.
 * @param columns. A collection of standard Columns.  The columns within a super column are defined in an adhoc manner.
 *                 Columns within a super column do not have to have matching structures (similarly named child columns).
 */
public class SuperColumn implements org.apache.thrift.TBase<SuperColumn, SuperColumn._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("SuperColumn");

  private static final org.apache.thrift.protocol.TField NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("name", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField COLUMNS_FIELD_DESC = new org.apache.thrift.protocol.TField("columns", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField DELETED_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("deleted_time", org.apache.thrift.protocol.TType.I64, (short)3);

  public ByteBuffer name; // required
  public List<Column> columns; // required
  public long deleted_time; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAME((short)1, "name"),
    COLUMNS((short)2, "columns"),
    DELETED_TIME((short)3, "deleted_time");

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
        case 1: // NAME
          return NAME;
        case 2: // COLUMNS
          return COLUMNS;
        case 3: // DELETED_TIME
          return DELETED_TIME;
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
  private static final int __DELETED_TIME_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NAME, new org.apache.thrift.meta_data.FieldMetaData("name", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.COLUMNS, new org.apache.thrift.meta_data.FieldMetaData("columns", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Column.class))));
    tmpMap.put(_Fields.DELETED_TIME, new org.apache.thrift.meta_data.FieldMetaData("deleted_time", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(SuperColumn.class, metaDataMap);
  }

  public SuperColumn() {
    this.deleted_time = 0L;

  }

  public SuperColumn(
    ByteBuffer name,
    List<Column> columns)
  {
    this();
    this.name = name;
    this.columns = columns;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public SuperColumn(SuperColumn other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetName()) {
      this.name = org.apache.thrift.TBaseHelper.copyBinary(other.name);
;
    }
    if (other.isSetColumns()) {
      List<Column> __this__columns = new ArrayList<Column>();
      for (Column other_element : other.columns) {
        __this__columns.add(new Column(other_element));
      }
      this.columns = __this__columns;
    }
    this.deleted_time = other.deleted_time;
  }

  public SuperColumn deepCopy() {
    return new SuperColumn(this);
  }

  @Override
  public void clear() {
    this.name = null;
    this.columns = null;
    this.deleted_time = 0L;

  }

  public byte[] getName() {
    setName(org.apache.thrift.TBaseHelper.rightSize(name));
    return name == null ? null : name.array();
  }

  public ByteBuffer bufferForName() {
    return name;
  }

  public SuperColumn setName(byte[] name) {
    setName(name == null ? (ByteBuffer)null : ByteBuffer.wrap(name));
    return this;
  }

  public SuperColumn setName(ByteBuffer name) {
    this.name = name;
    return this;
  }

  public void unsetName() {
    this.name = null;
  }

  /** Returns true if field name is set (has been assigned a value) and false otherwise */
  public boolean isSetName() {
    return this.name != null;
  }

  public void setNameIsSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  public int getColumnsSize() {
    return (this.columns == null) ? 0 : this.columns.size();
  }

  public java.util.Iterator<Column> getColumnsIterator() {
    return (this.columns == null) ? null : this.columns.iterator();
  }

  public void addToColumns(Column elem) {
    if (this.columns == null) {
      this.columns = new ArrayList<Column>();
    }
    this.columns.add(elem);
  }

  public List<Column> getColumns() {
    return this.columns;
  }

  public SuperColumn setColumns(List<Column> columns) {
    this.columns = columns;
    return this;
  }

  public void unsetColumns() {
    this.columns = null;
  }

  /** Returns true if field columns is set (has been assigned a value) and false otherwise */
  public boolean isSetColumns() {
    return this.columns != null;
  }

  public void setColumnsIsSet(boolean value) {
    if (!value) {
      this.columns = null;
    }
  }

  public long getDeleted_time() {
    return this.deleted_time;
  }

  public SuperColumn setDeleted_time(long deleted_time) {
    this.deleted_time = deleted_time;
    setDeleted_timeIsSet(true);
    return this;
  }

  public void unsetDeleted_time() {
    __isset_bit_vector.clear(__DELETED_TIME_ISSET_ID);
  }

  /** Returns true if field deleted_time is set (has been assigned a value) and false otherwise */
  public boolean isSetDeleted_time() {
    return __isset_bit_vector.get(__DELETED_TIME_ISSET_ID);
  }

  public void setDeleted_timeIsSet(boolean value) {
    __isset_bit_vector.set(__DELETED_TIME_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((ByteBuffer)value);
      }
      break;

    case COLUMNS:
      if (value == null) {
        unsetColumns();
      } else {
        setColumns((List<Column>)value);
      }
      break;

    case DELETED_TIME:
      if (value == null) {
        unsetDeleted_time();
      } else {
        setDeleted_time((Long)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NAME:
      return getName();

    case COLUMNS:
      return getColumns();

    case DELETED_TIME:
      return Long.valueOf(getDeleted_time());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NAME:
      return isSetName();
    case COLUMNS:
      return isSetColumns();
    case DELETED_TIME:
      return isSetDeleted_time();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SuperColumn)
      return this.equals((SuperColumn)that);
    return false;
  }

  public boolean equals(SuperColumn that) {
    if (that == null)
      return false;

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_columns = true && this.isSetColumns();
    boolean that_present_columns = true && that.isSetColumns();
    if (this_present_columns || that_present_columns) {
      if (!(this_present_columns && that_present_columns))
        return false;
      if (!this.columns.equals(that.columns))
        return false;
    }

    boolean this_present_deleted_time = true && this.isSetDeleted_time();
    boolean that_present_deleted_time = true && that.isSetDeleted_time();
    if (this_present_deleted_time || that_present_deleted_time) {
      if (!(this_present_deleted_time && that_present_deleted_time))
        return false;
      if (this.deleted_time != that.deleted_time)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder builder = new HashCodeBuilder();

    boolean present_name = true && (isSetName());
    builder.append(present_name);
    if (present_name)
      builder.append(name);

    boolean present_columns = true && (isSetColumns());
    builder.append(present_columns);
    if (present_columns)
      builder.append(columns);

    boolean present_deleted_time = true && (isSetDeleted_time());
    builder.append(present_deleted_time);
    if (present_deleted_time)
      builder.append(deleted_time);

    return builder.toHashCode();
  }

  public int compareTo(SuperColumn other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    SuperColumn typedOther = (SuperColumn)other;

    lastComparison = Boolean.valueOf(isSetName()).compareTo(typedOther.isSetName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.name, typedOther.name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetColumns()).compareTo(typedOther.isSetColumns());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColumns()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.columns, typedOther.columns);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDeleted_time()).compareTo(typedOther.isSetDeleted_time());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDeleted_time()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.deleted_time, typedOther.deleted_time);
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
        case 1: // NAME
          if (field.type == org.apache.thrift.protocol.TType.STRING) {
            this.name = iprot.readBinary();
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 2: // COLUMNS
          if (field.type == org.apache.thrift.protocol.TType.LIST) {
            {
              org.apache.thrift.protocol.TList _list4 = iprot.readListBegin();
              this.columns = new ArrayList<Column>(_list4.size);
              for (int _i5 = 0; _i5 < _list4.size; ++_i5)
              {
                Column _elem6; // required
                _elem6 = new Column();
                _elem6.read(iprot);
                this.columns.add(_elem6);
              }
              iprot.readListEnd();
            }
          } else { 
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case 3: // DELETED_TIME
          if (field.type == org.apache.thrift.protocol.TType.I64) {
            this.deleted_time = iprot.readI64();
            setDeleted_timeIsSet(true);
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
    if (this.name != null) {
      oprot.writeFieldBegin(NAME_FIELD_DESC);
      oprot.writeBinary(this.name);
      oprot.writeFieldEnd();
    }
    if (this.columns != null) {
      oprot.writeFieldBegin(COLUMNS_FIELD_DESC);
      {
        oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, this.columns.size()));
        for (Column _iter7 : this.columns)
        {
          _iter7.write(oprot);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    if (isSetDeleted_time()) {
      oprot.writeFieldBegin(DELETED_TIME_FIELD_DESC);
      oprot.writeI64(this.deleted_time);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SuperColumn(");
    boolean first = true;

    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      org.apache.thrift.TBaseHelper.toString(this.name, sb);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("columns:");
    if (this.columns == null) {
      sb.append("null");
    } else {
      sb.append(this.columns);
    }
    first = false;
    if (isSetDeleted_time()) {
      if (!first) sb.append(", ");
      sb.append("deleted_time:");
      sb.append(this.deleted_time);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (name == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'name' was not present! Struct: " + toString());
    }
    if (columns == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'columns' was not present! Struct: " + toString());
    }
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


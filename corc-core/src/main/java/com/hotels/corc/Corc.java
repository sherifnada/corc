/**
 * Copyright (C) 2015 Expedia Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.corc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link OrcStruct} wrapper, allowing access by field name with automatic {@link Writable} to Java type conversion
 * using the provided {@link ConverterFactory}.
 */
public class Corc implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(Corc.class);

  private final OrcSerde serde = new OrcSerde();
  private final SettableStructObjectInspector inspector;
  private final OrcStruct struct;
  private final ConverterFactory factory;
  private final Map<String, ValueMarshaller> cache = new HashMap<>();
  private final RecordIdentifier recordIdentifier;
  private TypeDescription typeDescription;


  /*
  why do we want typeInfo?
    - create inspector. used to:
      - create placeholder struct
      - create a value marshaller for a given field. To create a marshaller, we get the appropriate OrcStruct <-> Java
      converter, get the value of that field, and create a valueMarshaller(inspector, fieldValue, converter)

    - DefaultConverterFactory -- all these converters will need to start using TypeDescription instead of TypeInfo
   */
  public Corc(TypeDescription typeDescription, ConverterFactory factory) {
    LOG.debug("TypeInfo: {}", typeDescription);
    this.struct = new OrcStruct(typeDescription);
    this.factory = factory;
    this.recordIdentifier = new RecordIdentifier();
  }

  private ValueMarshaller getValueMarshaller(String fieldName) {
    ValueMarshaller valueMarshaller = cache.get(fieldName);

    if (valueMarshaller == null) {
      Writable writable = struct.getFieldValue(fieldName);
      TypeDescription fieldTypeDescription = getTypeDescriptionForFieldWithName(typeDescription, fieldName);
      if (writable == null) {
        valueMarshaller = ValueMarshaller.NULL;
      } else {
        Converter converter = factory.newConverter(fieldTypeDescription);

        valueMarshaller = new ValueMarshallerImpl(fieldName, converter);
      }
      cache.put(fieldName, valueMarshaller);
    }
    return valueMarshaller;
  }

  /**
   * Gets the value for {@code fieldName} converted to the appropriate java type
   *
   * @throws IOException
   */
  public Object get(String fieldName) throws IOException {
    Object value = getValueMarshaller(fieldName).getJavaObject(struct);
    LOG.debug("Fetched {}={}", fieldName, value);
    return value;
  }

  /**
   * Gets the raw {@link Writable} value for {@code fieldName}
   *
   * @throws IOException
   */
  public Object getWritable(String fieldName) {
    Object value = getValueMarshaller(fieldName).getWritableObject(struct);
    LOG.debug("Fetched writable {}={}", fieldName, value);
    return value;
  }

  /**
   * Sets the value for {@code fieldName}, first converting it to the appropriate {@link Writable} type
   *
   * @throws IOException
   */
  public void set(String fieldName, Object value) throws IOException {
    getValueMarshaller(fieldName).setWritableObject(struct, value);
    LOG.debug("Set {}={}", fieldName, value);
  }

  public RecordIdentifier getRecordIdentifier() {
    RecordIdentifier copy = new RecordIdentifier();
    copy.set(recordIdentifier);
    LOG.debug("Fetched recordIdentifier={}", recordIdentifier);
    return copy;
  }

  public void setRecordIdentifier(RecordIdentifier recordIdentifier) {
    this.recordIdentifier.set(recordIdentifier);
    LOG.debug("Set recordIdentifier={}", recordIdentifier);
  }

  public OrcStruct getOrcStruct() {
    return struct;
  }

  public SettableStructObjectInspector getInspector() {
    return inspector;
  }

  public Object serialize() {
    // Corc is writable, which should be all we need. So think we can just return this object?
    return serde.serialize(struct, inspector);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException(Corc.class.getName() + " cannot  be used for writing.");
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException(Corc.class.getName() + " cannot be used for reading.");
  }

  private TypeDescription getTypeDescriptionForFieldWithName(TypeDescription description, String fieldName) {
    for (int i = 0; i < description.getChildren().size(); i++) {
      if (description.getFieldNames().get(i).equals(fieldName)) {
        return description.getChildren().get(i);
      }
    }
  }
}
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

import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.mapred.OrcStruct;

class ValueMarshallerImpl<T> implements ValueMarshaller {

  private final String fieldName;
  private final Converter<T> converter;

  ValueMarshallerImpl(String fieldName, Converter<T> converter) {
    this.fieldName = fieldName;
    this.converter = converter;
  }

  @Override
  public Object getJavaObject(OrcStruct struct) throws UnexpectedTypeException {
    Object writable = struct.getFieldValue(fieldName);
    try {
      return converter.toJavaObject(writable);
    } catch (UnexpectedTypeException e) {
      throw new UnexpectedTypeException(writable, fieldName, e);
    }
  }

  @Override
  public Object getWritableObject(OrcStruct struct) {
    return struct.getFieldValue(fieldName);
  }

  @Override
  public void setWritableObject(OrcStruct struct, Object javaObject) throws UnexpectedTypeException {
    WritableComparable<T> writable;
    try {
      writable = converter.toWritableObject(javaObject);
    } catch (UnexpectedTypeException e) {
      throw new UnexpectedTypeException(javaObject, fieldName, e);
    }
    struct.setFieldValue(fieldName, writable);
  }
}
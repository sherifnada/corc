/**
 * Copyright 2015 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.corc.sarg;

import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;

class IsNullEvaluator<T extends Comparable<T>> extends Evaluator<T> {

  IsNullEvaluator(String fieldName) {
    super(fieldName);
  }

  @Override
  protected TruthValue evaluate(T value) {
    if (value == null) {
      return TruthValue.YES;
    }
    return TruthValue.NO;
  }

}

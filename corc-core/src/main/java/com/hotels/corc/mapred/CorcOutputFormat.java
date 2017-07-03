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
package com.hotels.corc.mapred;

import java.io.IOException;

import com.hotels.corc.Corc;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.mapred.OrcOutputFormat;
import org.apache.orc.mapred.OrcStruct;

/**
 * A wrapper for {@link OrcOutputFormat} to expose {@link Corc} as the value type instead of {@link OrcStruct}.
 * This enables column access by name instead of by position.
 */
public class CorcOutputFormat extends FileOutputFormat<NullWritable, Corc> {

  private final OrcOutputFormat orcOutputFormat = new OrcOutputFormat();

  @Override
  public RecordWriter<NullWritable, Corc> getRecordWriter(FileSystem fileSystem, JobConf conf, String name,
                                                          Progressable progress) throws IOException {
    String file = FileOutputFormat.getTaskOutputPath(conf, name).toString();
    RecordWriter<NullWritable, ?> writer = orcOutputFormat.getRecordWriter(fileSystem, conf, file, progress);
    return new CorcRecordWriter(writer);
  }

}

package com.hotels.corc.sarg;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;

public class SargUtils {
  private static int BUFFER_SIZE = 4 * 1024;
  private static final int MAX_BUFFER_SIZE = 10 * 1024 * 1024;

  /**
   * @param sarg
   * @return
   */
  public static String toKryo(SearchArgument sarg) {
    Output out = new Output(BUFFER_SIZE, MAX_BUFFER_SIZE);
    new Kryo().writeObject(out, sarg);
    out.close();
    return Base64.encodeBase64String(out.toBytes());
  }

  /**
   * @param kryo Base64 String encoding of the Kryo-serialized object
   * @return the {@link SearchArgument} object represented by the argument
   */
  public static SearchArgument fromKryo(String kryo) {
    Input input = new Input(Base64.decodeBase64(kryo));
    return new Kryo().readObject(input, SearchArgumentImpl.class);
  }
}

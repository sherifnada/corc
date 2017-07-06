package com.hotels.corc.sarg;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf.Type;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SargUtilsTest {
  private static final String COL0 = "column0";

  @Test
  public void testRoundtripSerialization() {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .equals(COL0, Type.STRING, "someValue")
        .end()
        .build();

    String sargKryo = SargUtils.toKryo(sarg);
    SearchArgument deserializedSarg = SargUtils.fromKryo(sargKryo);
    assertEquals(sargKryo, deserializedSarg);
  }
}

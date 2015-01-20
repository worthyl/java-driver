package com.datastax.driver.core;

public class OPPTokenIntegrationTest extends TokenIntegrationTest {
    public OPPTokenIntegrationTest() {
        super("-p ByteOrderedPartitioner", DataType.blob());
    }
}

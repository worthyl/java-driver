package com.datastax.driver.core;

public class RPTokenIntegrationTest extends TokenIntegrationTest {
    public RPTokenIntegrationTest() {
        super("-p RandomPartitioner", DataType.varint());
    }
}

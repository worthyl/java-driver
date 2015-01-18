package com.datastax.driver.core;

public class MetadataRandomTokenTest extends MetadataTokenTest {
    public MetadataRandomTokenTest() {
        super("-p RandomPartitioner", DataType.varint());
    }
}

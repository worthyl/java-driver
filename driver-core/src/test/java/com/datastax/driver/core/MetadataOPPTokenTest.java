package com.datastax.driver.core;

public class MetadataOPPTokenTest extends MetadataTokenTest {
    public MetadataOPPTokenTest() {
        super("-p ByteOrderedPartitioner", DataType.blob());
    }
}

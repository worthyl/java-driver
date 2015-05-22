/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class CustomPayloadTest extends CCMBridge.PerClassSingleNodeCluster {

    private CustomPayload customPayload = new CustomPayload.DefaultCustomPayload(
        ImmutableMap.of(
            "k1", new byte[]{ 1, 2, 3 },
            "k2", new byte[]{ 4, 5, 6 }
        ));

    // execute

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_string() throws Exception {
        ResultSet rows = session.executeWithPayload("SELECT c2 FROM t1 where c1 = 1", customPayload);
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_string_values() throws Exception {
        ResultSet rows = session.executeWithPayload("SELECT c2 FROM t1 where c1 = ?", customPayload, 1);
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_regular_statement() throws Exception {
        ResultSet rows = session.executeWithPayload(new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1), customPayload);
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_batch_statement() throws Exception {
        BatchStatement statement = new BatchStatement(BatchStatement.Type.LOGGED)
            .add(new SimpleStatement("INSERT INTO t1 (c1) VALUES (?)", 1))
            .add(new SimpleStatement("INSERT INTO t1 (c1) VALUES (?)", 2));
        ResultSet rows = session.executeWithPayload(statement, customPayload);
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    // execute async

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_string_async() throws Exception {
        ResultSet rows = session.executeAsyncWithPayload("SELECT c2 FROM t1 where c1 = 1", customPayload).get();
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_string_values_async() throws Exception {
        ResultSet rows = session.executeAsyncWithPayload("SELECT c2 FROM t1 where c1 = ?", customPayload, 1).get();
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_regular_statement_async() throws Exception {
        ResultSet rows = session.executeAsyncWithPayload(new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1), customPayload).get();
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_when_executing_batch_statement_async() throws Exception {
        BatchStatement statement = new BatchStatement(BatchStatement.Type.LOGGED)
            .add(new SimpleStatement("INSERT INTO t1 (c1) VALUES (?)", 1))
            .add(new SimpleStatement("INSERT INTO t1 (c1) VALUES (?)", 2));
        ResultSet rows = session.executeAsyncWithPayload(statement, customPayload).get();
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    // prepare

    /**
     * TODO enable after JAVA-776
     * @throws Exception
     */
    @Test(groups = "short", enabled = false)
    public void should_echo_custom_payload_when_preparing_statement() throws Exception {
        PreparedStatement ps = session.prepareWithPayload(new SimpleStatement("SELECT c2 FROM t1 where c1 = ?"), customPayload);
        assertCustomPayload(((CustomPayloadAwarePreparedStatement)ps).getCustomPayload());
        ResultSet rows = session.execute(ps.bind(1));
        assertCustomPayload(rows.getExecutionInfo().getCustomPayload());
    }

    /**
     * TODO enable after JAVA-776
     * @throws Exception
     */
    @Test(groups = "short", enabled = false)
    public void should_echo_custom_payload_when_preparing_statement_async() throws Exception {
        PreparedStatement ps = session.prepareAsyncWithPayload(new SimpleStatement("SELECT c2 FROM t1 where c1 = ?"), customPayload).get();
        assertCustomPayload(((CustomPayloadAwarePreparedStatement)ps).getCustomPayload());
        ResultSet rows = session.execute(ps.bind(1));
        assertCustomPayload(rows.getExecutionInfo().getCustomPayload());
    }

    // pagination

    @Test(groups = "short")
    public void should_echo_custom_payload_when_paginating() throws Exception {
        session.execute("INSERT INTO t1 (c1) VALUES (1)");
        session.execute("INSERT INTO t1 (c1) VALUES (2)");
        SimpleStatement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 IN (1,2)");
        statement.setFetchSize(1);
        ResultSet rows = session.executeWithPayload(statement, customPayload);
        rows.all();
        for (ExecutionInfo info : rows.getAllExecutionInfo()) {
            CustomPayload actual = info.getCustomPayload();
            assertCustomPayload(actual);
        }
    }

    // statement override - merge

    @Test(groups = "short")
    public void should_echo_custom_payload_in_statement() throws Exception {
        SimpleStatement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setCustomPayload(customPayload);
        ResultSet rows = session.execute(statement);
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertCustomPayload(actual);
    }

    @Test(groups = "short")
    public void should_echo_custom_payload_merged() throws Exception {
        CustomPayload.DefaultCustomPayload payload1 = new CustomPayload.DefaultCustomPayload(
            ImmutableMap.of(
                "k1", new byte[]{ 1, 2, 3 },
                "k2", new byte[]{ 4, 5, 6 }
            ));
        CustomPayload.DefaultCustomPayload payload2 = new CustomPayload.DefaultCustomPayload(
            ImmutableMap.of(
                "k2", new byte[]{ 6, 5, 4 },
                "k3", new byte[]{ 7, 8, 9 }
            ));
        SimpleStatement statement = new SimpleStatement("SELECT c2 FROM t1 where c1 = ?", 1);
        statement.setCustomPayload(payload1);
        ResultSet rows = session.executeWithPayload(statement, payload2);
        CustomPayload actual = rows.getExecutionInfo().getCustomPayload();
        assertThat(actual.asBytesMap()).containsOnly(
            entry("k1", new byte[]{ 1, 2, 3 }),
            entry("k2", new byte[]{ 6, 5, 4 }),
            entry("k3", new byte[]{ 7, 8, 9 })
        );
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        builder.withProtocolVersion(ProtocolVersion.V4);
        return builder;
    }

    @Override
    protected String getJvmArgs() {
        return "-Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler";
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singletonList("CREATE TABLE t1 (c1 int PRIMARY KEY, c2 text)");
    }

    private void assertCustomPayload(CustomPayload actual) {
        assertThat(actual.asBytesMap()).containsOnly(
            entry("k1", new byte[]{ 1, 2, 3 }),
            entry("k2", new byte[]{ 4, 5, 6 })
        );
    }

}

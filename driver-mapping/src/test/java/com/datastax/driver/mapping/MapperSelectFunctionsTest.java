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
package com.datastax.driver.mapping;

import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.mapping.annotations.*;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * Tests to ensure validity of {@code computed} option in
 * {@link com.datastax.driver.mapping.annotations.Column}
 */
public class MapperSelectFunctionsTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE user (key int primary key, v text)");
    }

    @Test(groups = "short")
    void should_fail_if_computed_field_is_not_right_type() {
        boolean getObjectFailed = false;
        try {
            Mapper<User3> mapper = new MappingManager(session).mapper(User3.class);
        } catch (IllegalArgumentException e) {
            getObjectFailed = true;
        }
        assertThat(getObjectFailed).isTrue();
    }

    @Test(groups = "short")
    void should_fail_if_field_not_existing_and_not_marked_computed() {
        boolean getObjectFailed = false;
        boolean setObjectFailed = false;
        Mapper<User2> mapper = new MappingManager(session).mapper(User2.class);
        try {
            mapper.save(new User2(42, "helloworld"));
        } catch (SyntaxError e) {
            setObjectFailed = true;
        }
        assertThat(setObjectFailed).isTrue();
        try {
            User2 saved = mapper.get(42);
        } catch (SyntaxError e) {
            getObjectFailed = true;
        }
        assertThat(getObjectFailed).isTrue();
    }

    @Test(groups = "short")
    void should_fetch_computed_fields() {
        Mapper<User> mapper = new MappingManager(session).mapper(User.class);
        mapper.save(new User(42, "helloworld"));
        User saved = mapper.get(42);
        assertThat(saved.getWriteTime()).isNotNull();
        assertThat(saved.getWriteTime()).isNotEqualTo(0);
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private int key;
        private String v;

        @Column(name = "writetime(v)", computed = true)
        long writeTime;

        public User() {
        }

        public User(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }

        public long getWriteTime() {
            return this.writeTime;
        }

        public void setWriteTime(long pk) {
            this.writeTime = pk;
        }
    }

    @Table(name = "user")
    public static class User2 {
        @PartitionKey
        private int key;
        private String v;

        @Column(name = "writetime(v)")
        long writeTime;

        public User2() {
        }

        public User2(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }

        public long getWriteTime() {
            return this.writeTime;
        }

        public void setWriteTime(long pk) {
            this.writeTime = pk;
        }
    }

    @Table(name = "user")
    public static class User3 {
        @PartitionKey
        private int key;
        private String v;

        @Column(name = "writetime(v)", computed = true)
        byte writeTime;

        public User3() {
        }

        public User3(int k, String val) {
            this.key = k;
            this.v = val;
        }

        public int getKey() {
            return this.key;
        }

        public void setKey(int pk) {
            this.key = pk;
        }

        public String getV() {
            return this.v;
        }

        public void setV(String val) {
            this.v = val;
        }

        public byte getWriteTime() {
            return this.writeTime;
        }

        public void setWriteTime(byte pk) {
            this.writeTime = pk;
        }
    }
}

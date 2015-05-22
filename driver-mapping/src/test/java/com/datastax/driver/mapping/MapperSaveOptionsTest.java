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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

public class MapperSaveOptionsTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE user (key int primary key, v text)");
    }

    @Test(groups = "short")
    void should_use_using_options_to_save() {
        Long tsValue = 906L;
        Mapper<User> mapper = new MappingManager(session).mapper(User.class);
        QueryType.SaveOptions options = new QueryType.SaveOptions().setTtlValue(45);
        options.setTimestampValue(tsValue);
        mapper.saveAsync(new User(42, "helloworld"), options);
        assertThat(mapper.get(42).getV()).isEqualTo("helloworld");
        Long tsReturned = session.execute("SELECT writetime(v) FROM user WHERE key=" + 42).one().getLong(0);
        assertThat(tsReturned).isEqualTo(tsValue);
    }

    @Test(groups = "short")
    void should_use_using_options_only_once() {
        Long tsValue = 1L;
        Mapper<User> mapper = new MappingManager(session).mapper(User.class);
        QueryType.SaveOptions options = new QueryType.SaveOptions();
        options.setTimestampValue(tsValue);
        mapper.save(new User(42, "helloworld"), options);
        mapper.save(new User(43, "test"));
        Long tsReturned = session.execute("SELECT writetime(v) FROM user WHERE key=" + 43).one().getLong(0);
        // Assuming we cannot go back in time (yet) and execute the write at ts=1
        assertThat(tsReturned).isNotEqualTo(tsValue);
    }

    @Table(name = "user")
    public static class User {
        @PartitionKey
        private int key;
        private String v;

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
    }
}

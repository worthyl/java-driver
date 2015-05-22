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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

/**
 * A custom payload to be included in request or response frames.
 * <p>
 * Custom payloads can be used to convey additional information
 * from client to server and back. Theyare sent in the form of a bytes map, i.e.,
 * a sequence of key-value pairs where keys are strings and
 * values are arbitrary byte arrays.
 * <p>
 * Custom payloads can be set at two different levels:
 * <ol>
 *     <li>at {@link Statement} level: using the {@link Statement#setCustomPayload(CustomPayload)} method;</li>
 *     <li>at {@link Session} level: using any of the {@link CustomPayloadAwareSession} API methods.</li>
 * </ol>
 * Payload entries specified at Session level take precedence over those specified at Statement level.
 * <p>
 * The default {@code QueryHandler} implementation server-side simply
 * ignores all custom payloads. For a custom payload sent by the driver
 * to be taken into account server-side, or to have the driver receive
 * a custom payload from the server, a custom {@code QueryHandler} must also be specified
 * server-side.
 * <p>
 * <strong>IMPORTANT:</strong> Custom payloads are available from protocol version 4 onwards.
 * Trying to include custom payloads in requests sent by the driver
 * under lower protocol versions will result in
 * {@link com.datastax.driver.core.exceptions.DriverInternalError}s being thrown.
 *
 * @see CustomPayloadAwareSession
 * @see Statement#getCustomPayload()
 * @see "https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec"
 */
public interface CustomPayload {

    /**
     * Return the custom payload as a map of strings to byte arrays.
     * @return
     */
    Map<String, byte[]> asBytesMap();

    /**
     * Merge the given payload with the current one.
     * Implementations should enforce that in case of duplicate entries,
     * entries from the given payload should override those in the current one.
     * @param other the payload to merge with the current one
     * @return a merged payload containing entries from both the current and the given payloads
     */
    CustomPayload merge(CustomPayload other);

    /**
     * Default implementation of CustomPayload.
     * Instances of this class are immutable and thread-safe.
     */
    final class DefaultCustomPayload implements CustomPayload {

        private final ImmutableMap<String, byte[]> map;

        public DefaultCustomPayload(Map<String, byte[]> map) {
            this.map = ImmutableMap.copyOf(map);
        }

        @Override
        public Map<String, byte[]> asBytesMap() {
            return map;
        }

        @Override
        public CustomPayload merge(CustomPayload other) {
            HashMap<String, byte[]> merged = new HashMap<String, byte[]>(map);
            merged.putAll(other.asBytesMap());
            return new DefaultCustomPayload(merged);
        }

    }

}

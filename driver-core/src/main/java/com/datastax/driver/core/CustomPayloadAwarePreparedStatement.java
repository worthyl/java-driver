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

public interface CustomPayloadAwarePreparedStatement extends PreparedStatement {

    /**
     * Return the custom payload that the server sent back with its response, if any,
     * or {@code null}, if the server did not include any custom payload.
     * <p>
     * <strong>IMPORTANT:</strong> Custom payloads are available from protocol version 4 onwards.
     * Under lower protocol versions, this method will always return {@code null}.
     *
     * @return the custom payload that the server sent back with its response, if any,
     * or {@code null}, if the server did not include any custom payload
     */
    CustomPayload getCustomPayload();
}

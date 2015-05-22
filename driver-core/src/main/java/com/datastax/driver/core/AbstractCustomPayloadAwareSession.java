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

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Abstract implementation of the {@link CustomPayloadAwareSession} interface.
 */
public abstract class AbstractCustomPayloadAwareSession extends AbstractSession implements CustomPayloadAwareSession {

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(String query, CustomPayload customPayload) {
        return execute(new SimpleStatement(query), customPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(String query, CustomPayload customPayload, Object... values) {
        return execute(new SimpleStatement(query, values), customPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet execute(Statement statement, CustomPayload customPayload) {
        return executeAsync(statement, customPayload).getUninterruptibly();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(String query, CustomPayload customPayload) {
        return executeAsync(new SimpleStatement(query), customPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(String query, CustomPayload customPayload, Object... values) {
        return executeAsync(new SimpleStatement(query, values), customPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepare(String query, CustomPayload customPayload) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsync(query, customPayload));
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepare(RegularStatement statement, CustomPayload customPayload) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsync(statement, customPayload));
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(final RegularStatement statement, CustomPayload customPayload) {
        if (statement.hasValues())
            throw new IllegalArgumentException("A statement to prepare should not have values");

        ListenableFuture<PreparedStatement> prepared = prepareAsync(statement.toString(), customPayload);
        return Futures.transform(prepared, new Function<PreparedStatement, PreparedStatement>() {
            @Override
            public PreparedStatement apply(PreparedStatement prepared) {
                ByteBuffer routingKey = statement.getRoutingKey();
                if (routingKey != null)
                    prepared.setRoutingKey(routingKey);
                prepared.setConsistencyLevel(statement.getConsistencyLevel());
                if (statement.isTracing())
                    prepared.enableTracing();
                prepared.setRetryPolicy(statement.getRetryPolicy());

                return prepared;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsync(Statement statement) {
        return executeAsync(statement, null);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        return prepareAsync(query, null);
    }

}

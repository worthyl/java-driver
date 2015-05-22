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
    public ResultSet executeWithPayload(String query, CustomPayload customPayload) {
        return executeWithPayload(new SimpleStatement(query), customPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet executeWithPayload(String query, CustomPayload customPayload, Object... values) {
        return executeWithPayload(new SimpleStatement(query, values), customPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet executeWithPayload(Statement statement, CustomPayload customPayload) {
        return executeAsyncWithPayload(statement, customPayload).getUninterruptibly();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsyncWithPayload(String query, CustomPayload customPayload) {
        return executeAsyncWithPayload(new SimpleStatement(query), customPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSetFuture executeAsyncWithPayload(String query, CustomPayload customPayload, Object... values) {
        return executeAsyncWithPayload(new SimpleStatement(query, values), customPayload);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareWithPayload(String query, CustomPayload customPayload) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsyncWithPayload(query, customPayload));
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareWithPayload(RegularStatement statement, CustomPayload customPayload) {
        try {
            return Uninterruptibles.getUninterruptibly(prepareAsyncWithPayload(statement, customPayload));
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ListenableFuture<PreparedStatement> prepareAsyncWithPayload(final RegularStatement statement, CustomPayload customPayload) {
        if (statement.hasValues())
            throw new IllegalArgumentException("A statement to prepare should not have values");

        ListenableFuture<PreparedStatement> prepared = prepareAsyncWithPayload(statement.toString(), customPayload);
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
        return executeAsyncWithPayload(statement, null);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        return prepareAsyncWithPayload(query, null);
    }

}

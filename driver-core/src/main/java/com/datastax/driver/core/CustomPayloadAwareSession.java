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

import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;

/**
 * A specialized {@link Session} that adds additional methods
 * that allow clients to send {@link CustomPayload custom payloads}.
 * <p>
 * Note that custom payload entries specified through any of this API methods
 * should take precedence over custom payloads specified at
 * {@link Statement#setCustomPayload(CustomPayload)} statement level}.
 * Implementors should actually merge the given payloads into
 * payloads specified at statement level, so that the resulting payload
 * could be retrieved, after execution, by simply inspecting
 * the statement's payload.
 * <p>
 * <strong>IMPORTANT:</strong> Custom payloads are available from protocol version 4 onwards.
 * Trying to include custom payloads in requests sent by the driver
 * under lower protocol versions will result in
 * {@link com.datastax.driver.core.exceptions.DriverInternalError}s being thrown.
 */
public interface CustomPayloadAwareSession extends Session {

    /**
     * Executes the provided query with the provided custom payload.
     *
     * This is a convenience method for {@code execute(new SimpleStatement(query), customPayload)}.
     *
     * @param query the CQL query to execute.
     * @param customPayload the custom payload to send with the request
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     */
    public ResultSet execute(String query, CustomPayload customPayload);

    /**
     * Executes the provided query using the provided values and the provided custom payload.
     *
     * This is a convenience method for {@code execute(new SimpleStatement(query, values), customPayload)}.
     *
     * @param query the CQL query to execute.
     * @param customPayload the custom payload to send with the request
     * @param values values required for the execution of {@code query}. See
     * {@link SimpleStatement#SimpleStatement(String, Object...)} for more detail.
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws UnsupportedFeatureException if version 1 of the protocol
     * is in use (i.e. if you've force version 1 through {@link Cluster.Builder#withProtocolVersion}
     * or you use Cassandra 1.2).
     */
    public ResultSet execute(String query, CustomPayload customPayload, Object... values);

    /**
     * Executes the provided query with the provided custom payload.
     *
     * This method blocks until at least some result has been received from the
     * database. However, for SELECT queries, it does not guarantee that the
     * result has been received in full. But it does guarantee that some
     * response has been received from the database, and in particular
     * guarantee that if the request is invalid, an exception will be thrown
     * by this method.
     *
     * @param statement the CQL query to execute (that can be any {@code Statement}).
     * @param customPayload the custom payload to send with the request
     * @return the result of the query. That result will never be null but can
     * be empty (and will be for any non SELECT query).
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, i.e. an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws UnsupportedFeatureException if the protocol version 1 is in use and
     * a feature not supported has been used. Features that are not supported by
     * the version protocol 1 include: BatchStatement, ResultSet paging and binary
     * values in RegularStatement.
     */
    public ResultSet execute(Statement statement, CustomPayload customPayload);

    /**
     * Executes the provided query asynchronously with the provided custom payload.
     * <p>
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query), customPayload)}.
     *
     * @param query the CQL query to execute.
     * @param customPayload the custom payload to send with the request
     * @return a future on the result of the query.
     */
    public ResultSetFuture executeAsync(String query, CustomPayload customPayload);

    /**
     * Executes the provided query asynchronously using the provided values and the provided custom payload.
     *
     * This is a convenience method for {@code executeAsync(new SimpleStatement(query, values), customPayload)}.
     *
     * @param query the CQL query to execute.
     * @param customPayload the custom payload to send with the request
     * @param values values required for the execution of {@code query}. See
     * {@link SimpleStatement#SimpleStatement(String, Object...)} for more detail.
     * @return a future on the result of the query.
     *
     * @throws UnsupportedFeatureException if version 1 of the protocol
     * is in use (i.e. if you've force version 1 through {@link Cluster.Builder#withProtocolVersion}
     * or you use Cassandra 1.2).
     */
    public ResultSetFuture executeAsync(String query, CustomPayload customPayload, Object... values);

    /**
     * Executes the provided query asynchronously with the provided custom payload.
     *
     * This method does not block. It returns as soon as the query has been
     * passed to the underlying network stack. In particular, returning from
     * this method does not guarantee that the query is valid or has even been
     * submitted to a live node. Any exception pertaining to the failure of the
     * query will be thrown when accessing the {@link ResultSetFuture}.
     * <p>
     * Note that for queries that doesn't return a result (INSERT, UPDATE and
     * DELETE), you will need to access the ResultSetFuture (that is call one of
     * its get method to make sure the query was successful.
     *
     * @param statement the CQL query to execute (that can be either any {@code Statement}.
     * @param customPayload the custom payload to send with the request
     * @return a future on the result of the query.
     *
     * @throws UnsupportedFeatureException if the protocol version 1 is in use and
     * a feature not supported has been used. Features that are not supported by
     * the version protocol 1 include: BatchStatement, ResultSet paging and binary
     * values in RegularStatement.
     */
    public ResultSetFuture executeAsync(Statement statement, CustomPayload customPayload);

    /**
     * Prepares the provided query string with the provided custom payload.
     *
     * @param query the CQL query string to prepare
     * @param customPayload the custom payload to send with the request
     * @return the prepared statement corresponding to {@code query}.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to prepare this query.
     */
    public PreparedStatement prepare(String query, CustomPayload customPayload);

    /**
     * Prepares the provided query with the provided custom payload.
     * <p>
     * This method is essentially a shortcut for {@code prepare(statement.getQueryString(), customPayload)},
     * but note that the resulting {@code PreparedStatement} will inherit the query properties
     * set on {@code statement}. Concretely, this means that in the following code:
     * <pre>
     *   RegularStatement toPrepare = new SimpleStatement("SELECT * FROM test WHERE k=?").setConsistencyLevel(ConsistencyLevel.QUORUM);
     *   PreparedStatement prepared = session.prepare(toPrepare);
     *   session.execute(prepared.bind("someValue"));
     * </pre>
     * the final execution will be performed with Quorum consistency.
     * <p>
     * Please note that if the same CQL statement is prepared more than once, all
     * calls to this method will return the same {@code PreparedStatement} object
     * but the method will still apply the properties of the prepared
     * {@code Statement} to this object.
     *
     * @param statement the statement to prepare
     * @param customPayload the custom payload to send with the request
     * @return the prepared statement corresponding to {@code statement}.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to prepare this statement.
     * @throws IllegalArgumentException if {@code statement.getValues() != null}
     * (values for executing a prepared statement should be provided after preparation
     * though the {@link PreparedStatement#bind} method or through a corresponding
     * {@link BoundStatement}).
     */
    public PreparedStatement prepare(RegularStatement statement, CustomPayload customPayload);

    /**
     * Prepares the provided query string asynchronously with the provided custom payload.
     * <p>
     * This method is equivalent to {@link #prepare(String, CustomPayload)} except that it
     * does not block but return a future instead. Any error during preparation will
     * be thrown when accessing the future, not by this method itself.
     *
     * @param query the CQL query string to prepare
     * @param customPayload the custom payload to send with the request
     * @return a future on the prepared statement corresponding to {@code query}.
     */
    public ListenableFuture<PreparedStatement> prepareAsync(String query, CustomPayload customPayload);

    /**
     * Prepares the provided query asynchronously with the provided custom payload.
     * <p>
     * This method is essentially a shortcut for {@code prepareAsync(statement.getQueryString(), customPayload)},
     * but with the additional effect that the resulting {@code
     * PreparedStatement} will inherit the query properties set on {@code statement}.
     * <p>
     * Please note that if the same CQL statement is prepared more than once, all
     * calls to this method will return the same {@code PreparedStatement} object
     * but the method will still apply the properties of the prepared
     * {@code Statement} to this object.
     *
     * @param statement the statement to prepare
     * @param customPayload the custom payload to send with the request
     * @return a future on the prepared statement corresponding to {@code statement}.
     *
     * @see CustomPayloadAwareSession#prepare(RegularStatement)
     *
     * @throws IllegalArgumentException if {@code statement.getValues() != null}
     * (values for executing a prepared statement should be provided after preparation
     * though the {@link PreparedStatement#bind} method or through a corresponding
     * {@link BoundStatement}).
     */
    public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement, CustomPayload customPayload);

}

/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Objects;

/**
 * A range of tokens (start exclusive and end exclusive) on the Cassandra ring.
 * <p>
 * Note that a range where start = end is considered empty.
 * <p>
 * If you need to query all the partitions in a range, be sure to use the following pattern to properly handle all corner cases:
 * <pre>
 *     &nbsp;{@code
 *     PreparedStatement between = session.prepare("SELECT i FROM foo WHERE token(i) > :start and token(i) <= :end");
 *     PreparedStatement after = session.prepare("SELECT i FROM foo WHERE token(i) > :start");
 *     PreparedStatement before = session.prepare("SELECT i FROM foo WHERE token(i) <= :end");
 *
 *     Token start = range.getStart(), end = range.getEnd();
 *     if (end.isMinToken() {
 *         session.execute(after.bind()
 *             .setBytesUnsafe("start", start.serialize()));
 *     } else if (start.compareTo(end) < 0) {
 *         session.execute(between.bind()
 *             .setBytesUnsafe("start", start.serialize())
 *             .setBytesUnsafe("end", end.serialize()));
 *     } else {
 *         // The range wraps around the end of the ring.
 *         // Two queries (combine the results depending on how you consume them).
 *         session.execute(after.bind()
 *             .setBytesUnsafe("start", start.serialize()));
 *         session.execute(before.bind()
 *             .setBytesUnsafe("end", end.serialize()));
 *     }
 * }
 * </pre>
 */
public final class TokenRange {
    private final Token start;
    private final Token end;
    private final Token.Factory factory;

    TokenRange(Token start, Token end, Token.Factory factory) {
        this.start = start;
        this.end = end;
        this.factory = factory;
    }

    /**
     * Return the start of the range.
     *
     * @return the start of the range (exclusive).
     */
    public Token getStart() {
        return start;
    }

    /**
     * Return the end of the range.
     *
     * @return the end of the range (inclusive).
     */
    public Token getEnd() {
        return end;
    }

    /**
     * Split this range into a number of smaller ranges of equal "size" (referring to the number of tokens, not the actual amount of data).
     *
     * @param numberOfSplits the number of splits to create.
     * @return the splits.
     */
    public List<TokenRange> splitEvenly(int numberOfSplits) {
        List<TokenRange> tokenRanges = new ArrayList<TokenRange>();
        List<Token> splitPoints = factory.split(start, end, numberOfSplits);
        Token splitStart = start;
        for (Token splitEnd : splitPoints) {
            tokenRanges.add(new TokenRange(splitStart, splitEnd, factory));
            splitStart = splitEnd;
        }
        tokenRanges.add(new TokenRange(splitStart, end, factory));
        return tokenRanges;
    }

    /**
     * Returns whether this range wraps around the maximum token.
     *
     * @return whether this range wraps around.
     */
    public boolean isWrappedAround() {
        return start.compareTo(end) > 0;
    }

    /**
     * Returns whether this range intersects another one.
     *
     * @param that the other range.
     * @return whether they intersect.
     */
    public boolean intersects(TokenRange that) {
        // Empty ranges never intersect any other range
        if (this.start.equals(this.end) || that.start.equals(that.end))
            return false;

        return this.contains(that.start, true)
            || this.contains(that.end, false)
            || that.contains(this.start, true)
            || that.contains(this.end, false);
    }

    private boolean contains(Token token, boolean isStart) {
        boolean isAfterStart = isStart ? token.compareTo(start) >= 0 : token.compareTo(start) > 0;
        boolean isBeforeEnd = isStart ? token.compareTo(end) < 0 : token.compareTo(end) <= 0;
        return isWrappedAround()
            ? isAfterStart || isBeforeEnd
            : isAfterStart && isBeforeEnd;
    }

    /**
     * Merges this range with another one.
     *
     * @param that the other range. It should either intersect this one or be adjacent; in other
     *             words, the resulting merge should not include tokens that are in neither of the
     *             original ranges.
     * @return the resulting range.
     *
     * @throws IllegalArgumentException if the other range is not intersecting or adjacent.
     */
    public TokenRange mergeWith(TokenRange that) {
        if (!(this.intersects(that) || this.end.equals(that.start) || that.end.equals(this.start)))
            throw new IllegalArgumentException(String.format(
                "Can't merge %s with %s because they neither intersect nor are adjacent",
                this, that));

        Token mergedStart, mergedEnd;
        if (this.end.equals(that.start)) {
            mergedStart = this.start;
            mergedEnd = that.end;
        } else if (that.end.equals(this.start)) {
            mergedStart = that.start;
            mergedEnd = this.end;
        } else {
            mergedStart = this.contains(that.start, true) ? this.start : that.start;
            mergedEnd = this.contains(that.end, false) ? this.end : that.end;
        }
        return new TokenRange(mergedStart, mergedEnd, factory);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this)
            return true;
        if (other instanceof TokenRange) {
            TokenRange that = (TokenRange)other;
            return Objects.equal(this.start, that.start) &&
                Objects.equal(this.end, that.end);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(start, end);
    }

    @Override
    public String toString() {
        return String.format("TokenRange(%s, %s)", start, end);
    }
}

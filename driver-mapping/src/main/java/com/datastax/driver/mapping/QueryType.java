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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Objects;

import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

class QueryType {

    private enum Kind { SAVE, GET, DEL, SLICE, REVERSED_SLICE };
    private final Kind kind;

    // For slices
    private final int startBoundSize;
    private final boolean startInclusive;
    private final int endBoundSize;
    private final boolean endInclusive;

    public static final QueryType SAVE = new QueryType(Kind.SAVE);
    public static final QueryType DEL = new QueryType(Kind.DEL);
    public static final QueryType GET = new QueryType(Kind.GET);

    private QueryType(Kind kind) {
        this(kind, 0, false, 0, false);
    }

    private QueryType(Kind kind, int startBoundSize, boolean startInclusive, int endBoundSize, boolean endInclusive) {
        this.kind = kind;
        this.startBoundSize = startBoundSize;
        this.startInclusive = startInclusive;
        this.endBoundSize = endBoundSize;
        this.endInclusive = endInclusive;
    }

    public static QueryType slice(int startBoundSize, boolean startInclusive, int endBoundSize, boolean endInclusive, boolean reversed) {
        return new QueryType(reversed ? Kind.REVERSED_SLICE : Kind.SLICE, startBoundSize, startInclusive, endBoundSize, endInclusive);
    }

    String makePreparedQueryString(TableMetadata table, EntityMapper<?> mapper, SaveOptions options) {
        switch (kind) {
            case SAVE:
                {
                    Insert insert = table == null
                                  ? insertInto(mapper.getKeyspace(), mapper.getTable())
                                  : insertInto(table);
                    for (ColumnMapper<?> cm : mapper.allColumns())
                        insert.value(cm.getColumnName(), bindMarker());
                    if (options != null){
                        addSaveOptions(insert, options);
                    }
                    return insert.toString();
                }
            case GET:
                {
                    Select select = table == null
                                  ? select().all().from(mapper.getKeyspace(), mapper.getTable())
                                  : select().all().from(table);
                    Select.Where where = select.where();
                    for (int i = 0; i < mapper.primaryKeySize(); i++)
                        where.and(eq(mapper.getPrimaryKeyColumn(i).getColumnName(), bindMarker()));
                    return select.toString();
                }
            case DEL:
                {
                    Delete delete = table == null
                                  ? delete().all().from(mapper.getKeyspace(), mapper.getTable())
                                  : delete().all().from(table);
                    Delete.Where where = delete.where();
                    for (int i = 0; i < mapper.primaryKeySize(); i++)
                        where.and(eq(mapper.getPrimaryKeyColumn(i).getColumnName(), bindMarker()));
                    return delete.toString();
                }
            case SLICE:
            case REVERSED_SLICE:
                {
                    Select select = table == null
                                  ? select().all().from(mapper.getKeyspace(), mapper.getTable())
                                  : select().all().from(table);
                    Select.Where where = select.where();
                    for (int i = 0; i < mapper.partitionKeys.size(); i++)
                        where.and(eq(mapper.partitionKeys.get(i).getColumnName(), bindMarker()));

                    if (startBoundSize > 0) {
                        if (startBoundSize == 1) {
                            String name = mapper.clusteringColumns.get(0).getColumnName();
                            where.and(startInclusive ? gte(name, bindMarker()) : gt(name, bindMarker()));
                        } else {
                            List<String> names = new ArrayList<String>(startBoundSize);
                            List<Object> values = new ArrayList<Object>(startBoundSize);
                            for (int i = 0; i < startBoundSize; i++) {
                                names.add(mapper.clusteringColumns.get(i).getColumnName());
                                values.add(bindMarker());
                            }
                            where.and(startInclusive ? gte(names, values) : gt(names, values));
                        }
                    }

                    if (endBoundSize > 0) {
                        if (endBoundSize == 1) {
                            String name = mapper.clusteringColumns.get(0).getColumnName();
                            where.and(endInclusive ? gte(name, bindMarker()) : gt(name, bindMarker()));
                        } else {
                            List<String> names = new ArrayList<String>(endBoundSize);
                            List<Object> values = new ArrayList<Object>(endBoundSize);
                            for (int i = 0; i < endBoundSize; i++) {
                                names.add(mapper.clusteringColumns.get(i).getColumnName());
                                values.add(bindMarker());
                            }
                            where.and(endInclusive ? lte(names, values) : lt(names, values));
                        }
                    }

                    select = select.limit(bindMarker());

                    if (kind == Kind.REVERSED_SLICE)
                        select = select.orderBy(desc(mapper.clusteringColumns.get(0).getColumnName()));

                    return select.toString();
                }
        }
        throw new AssertionError();
    }
    
    private void addSaveOptions(Insert insert, SaveOptions so){
        if (so.getTtlValue() != -1 && so.getTimestampValue() != -1) {
            insert.using(ttl(bindMarker("ttlvalue"))).and(timestamp(bindMarker("tsvalue")));
        } else if (so.getTtlValue() != -1) {
            insert.using(ttl(bindMarker("ttlvalue")));
        } else if (so.getTimestampValue() != -1) {
            insert.using(timestamp(bindMarker("tsvalue")));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        QueryType that = (QueryType)obj;
        return kind == that.kind
            && startBoundSize == that.startBoundSize
            && startInclusive == that.startInclusive
            && endBoundSize == that.endBoundSize
            && endInclusive == that.endInclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(kind, startBoundSize, startInclusive, endBoundSize, endInclusive);
    }

    /**
     * An object to allow defining specific options during a
     * {@link com.datastax.driver.mapping.Mapper#save} operation.
     *
     * The options will be added as : 'INSERT [...] USING option-name option-value [AND option-name option value ...].
     */
    public static class SaveOptions{
        private int ttlValue;
        private long timestampValue;

        /**
         * Creates an empty object to set up.
         */
        public SaveOptions(){
            ttlValue = -1;
            timestampValue = -1;
        }

        /**
         * Get the TTL value configured in the object.
         *
         * @return the TTL value.
         */
        public int getTtlValue() {
            return ttlValue;
        }

        /**
         * Set the TTL value to use for the aimed save operation.
         *
         * NOTE : To reset this field and giving it no effect this needs to be set at -1.
         * @param ttlValue
         * @return the SaveOptions object affected by the set of the field.
         */
        public SaveOptions setTtlValue(int ttlValue) {
            this.ttlValue = ttlValue;
            return this;
        }

        /**
         * Get the TTL value configured in the object.
         *
         * @return the TTL value.
         */
        public long getTimestampValue() {
            return timestampValue;
        }

        /**
         * Set the TIMESTAMP value to use for the aimed save operation.
         * <p/>
         * NOTE : To reset this field and giving it no effect this needs to be set at -1.
         *
         * @param timestampValue
         * @return the SaveOptions object affected by the set of the field.
         */
        public SaveOptions setTimestampValue(long timestampValue) {
            this.timestampValue = timestampValue;
            return this;
        }
    }
}

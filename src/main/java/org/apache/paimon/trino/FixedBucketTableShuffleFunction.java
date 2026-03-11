/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.RowKind;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/** Trino {@link BucketFunction}. */
public class FixedBucketTableShuffleFunction implements BucketFunction {

    private final int workerCount;
    private final int bucketCount;
    private final boolean isRowId;
    private final ThreadLocal<Projection> projectionContext;
    private final org.apache.paimon.bucket.BucketFunction paimonBucketFunction;

    public FixedBucketTableShuffleFunction(
            List<Type> partitionChannelTypes,
            TrinoPartitioningHandle partitioningHandle,
            int workerCount) {

        TableSchema schema = partitioningHandle.getOriginalSchema();
        this.isRowId =
                partitionChannelTypes.size() == 1
                        && partitionChannelTypes.get(0) instanceof RowType;
        if (isRowId) {
            // UPDATE path: RowBlock is unwrapped to primary key fields;
            // project bucket keys out of them
            this.projectionContext =
                    ThreadLocal.withInitial(
                            () ->
                                    CodeGenUtils.newProjection(
                                            schema.logicalPrimaryKeysType(), schema.bucketKeys()));
        } else {
            // INSERT path: page contains only bucket key columns
            this.projectionContext =
                    ThreadLocal.withInitial(
                            () ->
                                    CodeGenUtils.newProjection(
                                            schema.logicalBucketKeyType(), schema.bucketKeys()));
        }
        CoreOptions coreOptions = new CoreOptions(schema.options());
        this.bucketCount = coreOptions.bucket();
        this.paimonBucketFunction =
                org.apache.paimon.bucket.BucketFunction.create(
                        coreOptions, schema.logicalBucketKeyType());
        this.workerCount = workerCount;
    }

    @Override
    public int getBucket(Page page, int position) {
        if (isRowId) {
            RowBlock rowBlock = (RowBlock) page.getBlock(0);
            try {
                Method method = RowBlock.class.getDeclaredMethod("getRawFieldBlocks");
                method.setAccessible(true);
                page = new Page(rowBlock.getPositionCount(), (Block[]) method.invoke(rowBlock));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        TrinoRow trinoRow = new TrinoRow(page.getSingleValuePage(position), RowKind.INSERT);
        BinaryRow pk = projectionContext.get().apply(trinoRow);
        // paimon 1.3.1 no longer support KeyAndBucketExtractor, should use BucketFunction
        int bucket = paimonBucketFunction.bucket(pk, bucketCount);
        return bucket % workerCount;
    }
}

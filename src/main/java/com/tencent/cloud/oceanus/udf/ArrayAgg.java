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

package com.tencent.cloud.oceanus.udf;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** ARRAY_AGG 基类. */
public abstract class ArrayAgg<T>
        extends AggregateFunction<ArrayData, ArrayAgg.ArrayAggAccumulator<T>> {

    public abstract DataType getElementDataType();

    public DataType getAccumulatorDataType() {
        return DataTypes.STRUCTURED(
                ArrayAggAccumulator.class,
                DataTypes.FIELD("list", ListView.newListViewDataType(getElementDataType())),
                DataTypes.FIELD("retractList", ListView.newListViewDataType(getElementDataType())));
    }

    public DataType getOutputDataType() {
        return DataTypes.ARRAY(getElementDataType()).bridgedTo(ArrayData.class);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .typedArguments(Collections.singletonList(getElementDataType()))
                .accumulatorTypeStrategy(TypeStrategies.explicit(getAccumulatorDataType()))
                .outputTypeStrategy(TypeStrategies.explicit(getOutputDataType()))
                .build();
    }

    @Override
    public ArrayData getValue(ArrayAggAccumulator<T> accumulator) {
        try {
            return new GenericArrayData(accumulator.list.getList().toArray());
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public ArrayAggAccumulator<T> createAccumulator() {
        final ArrayAggAccumulator<T> acc = new ArrayAggAccumulator<T>();
        acc.list = new ListView<>();
        acc.retractList = new ListView<>();

        return acc;
    }

    public void accumulate(ArrayAggAccumulator<T> acc, T value) throws Exception {
        if (value != null) {
            acc.list.add(value);
        }
    }

    public void retract(ArrayAggAccumulator<T> acc, T value) throws Exception {
        if (value != null) {
            if (!acc.list.remove(value)) {
                acc.retractList.add(value);
            }
        }
    }

    public void merge(ArrayAggAccumulator<T> acc, Iterable<ArrayAggAccumulator<T>> its)
            throws Exception {
        for (ArrayAggAccumulator<T> otherAcc : its) {
            // merge list of acc and other
            List<T> buffer = new ArrayList<>();
            for (T element : acc.list.get()) {
                buffer.add(element);
            }
            for (T element : otherAcc.list.get()) {
                buffer.add(element);
            }
            // merge retract list of acc and other
            List<T> retractBuffer = new ArrayList<>();
            for (T element : acc.retractList.get()) {
                retractBuffer.add(element);
            }
            for (T element : otherAcc.retractList.get()) {
                retractBuffer.add(element);
            }

            // merge list & retract list
            List<T> newRetractBuffer = new ArrayList<>();
            for (T element : retractBuffer) {
                if (!buffer.remove(element)) {
                    newRetractBuffer.add(element);
                }
            }

            // update to acc
            acc.list.clear();
            acc.list.addAll(buffer);
            acc.retractList.clear();
            acc.retractList.addAll(newRetractBuffer);
        }
    }

    public void resetAccumulator(ArrayAggAccumulator<T> acc) {
        acc.list.clear();
        acc.retractList.clear();
    }

    /** Accumulator for ARRAY_ARG with retraction. */
    public static class ArrayAggAccumulator<T> {
        public ListView<T> list;
        public ListView<T> retractList;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ArrayAggAccumulator<?> that = (ArrayAggAccumulator<?>) o;
            return Objects.equals(list, that.list) && Objects.equals(retractList, that.retractList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(list, retractList);
        }
    }
}

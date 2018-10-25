/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.processor.customwindow;

import com.hazelcast.jet.aggregate.AggregateOperation1;

import javax.annotation.Nonnull;

class CountWindow<T, K, A> implements CustomWindowHandler<T, K, CountWindow.State<A>> {
    private final int maxCount;
    private final AggregateOperation1<T, A, ?> aggrOp;

    public CountWindow(int maxCount, AggregateOperation1<T, A, ?> aggrOp) {
        this.maxCount = maxCount;
        this.aggrOp = aggrOp;
    }

    @Override
    public void onItem(@Nonnull T item, long timestamp, @Nonnull WindowSet<State<A>> windows,
                          @Nonnull HandlerContext handlerContext) {
        State<A> state = windows.getOrCreate(Long.MIN_VALUE, Long.MAX_VALUE, () -> new State(aggrOp.createFn().get()));
        aggrOp.accumulateFn().accept(state.acc, item);
        state.count++;
        if (state.count == maxCount) {
            state.count = 0;
            windows.mark(Long.MIN_VALUE, Long.MAX_VALUE, true, true);
        }
    }

    @Override
    public boolean onWatermark(long watermarkTime, long windowStart, long windowEnd, State<A> accumulator, @Nonnull HandlerContext handlerContext) {
        return false;
    }

    static final class State<A> {
        int count;
        A acc;

        public State(A acc) {
            this.acc = acc;
        }
    }
}

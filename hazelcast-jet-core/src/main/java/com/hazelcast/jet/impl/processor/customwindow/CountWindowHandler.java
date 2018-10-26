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

import com.hazelcast.util.MutableLong;

import javax.annotation.Nonnull;

class CountWindowHandler<T, A> implements CustomWindowHandler<T, A, MutableLong> {
    private final int maxCount;

    public CountWindowHandler(int maxCount) {
        this.maxCount = maxCount;
    }

    @Override
    public void onItem(@Nonnull T item, long timestamp, @Nonnull WindowSet<T, A, MutableLong> windows) {
        MutableLong state = windows.accumulate(item, Long.MIN_VALUE, Long.MAX_VALUE, MutableLong::new);
        state.value++;
        if (state.value == maxCount) {
            state.value = 0;
            windows.mark(Long.MIN_VALUE, Long.MAX_VALUE, FIRE_AND_PURGE);
        }
    }

    @Override
    public int onWatermark(long watermarkTime, long windowStart, long windowEnd, MutableLong state) {
        return NO_ACTION;
    }
}

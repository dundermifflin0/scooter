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

import javax.annotation.Nonnull;

public interface CustomWindowHandler<T, A, S> {

    int NO_ACTION = 0;
    int FIRE = 1;
    int PURGE = 2;
    int FIRE_AND_PURGE = 3;

    void onItem(@Nonnull T item, long timestamp, @Nonnull WindowSet<T, A, S> windows);

    int onWatermark(long watermarkTime, long windowStart, long windowEnd, S state);

//    default void onTryProcess(@Nonnull Map<K, WindowSet<A>> state) {
//    }

    static boolean isFire(int action) {
        return (action & FIRE) != 0;
    }

    static boolean isPurge(int action) {
        return (action & PURGE) != 0;
    }
}

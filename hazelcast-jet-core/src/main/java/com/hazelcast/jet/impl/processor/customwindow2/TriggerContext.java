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

package com.hazelcast.jet.impl.processor.customwindow2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

public interface TriggerContext<S> {
    @Nullable
    S state();

    @Nonnull
    S state(Supplier<S> createStateFn);

    void setState(@Nullable S newState);

    /**
     * There can be at most one event timer for each key and window scheduled.
     * Scheduling another timer for the same key and window will remove the
     * existing one.
     */
    void scheduleEventTimeTimer(long time);

    void scheduleSystemTimeTimer(long time);
}

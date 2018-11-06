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

import com.hazelcast.jet.impl.processor.customwindow2.CustomWindowP.WindowDef;

import javax.annotation.Nullable;
import java.io.Serializable;

public interface Trigger<IN, S> extends Serializable {

    enum TriggerAction {
        NO_ACTION(false, false),
        EMIT(true, false),
        EVICT(false, true),
        EMIT_AND_EVICT(true, true);

        final boolean fire;
        final boolean purge;

        TriggerAction(boolean fire, boolean purge) {
            this.fire = fire;
            this.purge = purge;
        }
    }

    /**
     * Create a new state object. Can be null if no state is needed. One state
     * object will be created for each key and window. It must be a mutable
     * object, the other trigger methods will be able to mutate it, but not to
     * replace it.
     */
    @Nullable
    default S createState() throws Exception {
        return null;
    }

    /**
     * Merge {@code otherState} into {@code state}.
     * @param state State to merged to
     * @param otherState State to be merged
     */
    default void mergeState(S state, S otherState) {
        if (otherState == null) {
            return;
        }
        throw new UnsupportedOperationException("Trigger.mergeState() must be implemented for merging window set");
    }

    /**
     * Method will be called for each item+window when the item is received.
     * <p>
     * Default implementation schedules event time timer for the end of the
     * window.
      */
    default TriggerAction onItem(IN item, long timestamp, WindowDef window, S state, Timers timers) throws Exception {
        timers.scheduleEventTimeTimer(window.end());
        return TriggerAction.NO_ACTION;
    }

    /**
     * Method will be called to handle event time timer.
     *
     * @param time   The time the timer was scheduled to
     * @param window The window definition for the event
     * @param state The trigger state for this window+key tuple (as created by
     *          {@link #createState()}
     * @param timers Callbacks to schedule new timers
     */
    default TriggerAction onEventTime(long time, WindowDef window, S state, Timers timers) throws Exception {
        return TriggerAction.EMIT_AND_EVICT;
    }

    /**
     * Method will be called to handle system time timer.
     *
     * @param time   The time the timer was scheduled to
     * @param window The window definition for the event
     * @param state The trigger state for this window+key tuple (as created by
     *          {@link #createState()}
     * @param timers Callbacks to schedule new timers
     */
    default TriggerAction onSystemTime(long time, WindowDef window, S state, Timers timers) throws Exception {
        return TriggerAction.NO_ACTION;
    }

    interface Timers {
        /**
         * There can be at most one event timer for each key and window scheduled.
         * Scheduling another timer for the same key and window will remove the
         * existing one.
         */
        void scheduleEventTimeTimer(long time);

        /**
         * Note that after restoring from snapshot the timer will be restored
         * for the same instant, which can be long in the past.
         */
        void scheduleSystemTimeTimer(long time);
    }
}

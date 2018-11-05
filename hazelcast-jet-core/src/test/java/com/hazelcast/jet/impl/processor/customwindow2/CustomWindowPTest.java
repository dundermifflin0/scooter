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

import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import com.hazelcast.jet.core.test.TestSupport;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.processor.customwindow2.CustomWindowP.WindowDef;
import com.hazelcast.util.MutableLong;
import org.junit.Test;

import java.util.Collection;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.TestUtil.wm;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CustomWindowPTest {

    private final LongAccumulator clock = new LongAccumulator();

    @Test
    public void test_eventTimer() {
        DistributedSupplier<Processor> processor = () -> processor(
                (item, ts) -> singletonList(new WindowDef(ts, ts + 1)),
                new Trigger<Long, MutableLong>() { });
        TestSupport.verifyProcessor(processor)
                   .input(asList(
                           1L,
                           wm(2)
                   ))
                   .expectOutput(asList(
                           new TimestampedEntry(2, "key", 1L),
                           wm(2)
                   ));
    }

    @Test
    public void test_systemTimer() throws Exception {
        CustomWindowP processor = processor(
                (item, ts) -> singletonList(new WindowDef(ts, ts + 1)),
                new Trigger<Long, MutableLong>() {
                    @Override
                    public TriggerAction onItem(Long item, long timestamp, WindowDef window, MutableLong state, Timers timers) {
                        timers.scheduleSystemTimeTimer(clock.get() + 10);
                        return TriggerAction.NO_ACTION;
                    }

                    @Override
                    public TriggerAction onSystemTime(long time, WindowDef window, MutableLong state, Timers timers) {
                        return TriggerAction.EMIT_AND_EVICT;
                    }
                });

        TestOutbox outbox = new TestOutbox(1024);
        processor.init(outbox, new TestProcessorContext());
        processor.tryProcess(0, 1L);
        processor.tryProcess();
        assertTrue(outbox.queue(0).isEmpty());
        clock.add(10);
        processor.tryProcess();
        assertEquals(1, outbox.queue(0).size());
    }

    private CustomWindowP<Long, String, LongAccumulator, Long, MutableLong, TimestampedEntry> processor(
            DistributedBiFunction<Long, Long, Collection<WindowDef>> windowFn,
            Trigger<Long, MutableLong> trigger
    ) {
        return new CustomWindowP<>(
                singletonList((Long i) -> i),
                singletonList(i -> "key"),
                counting(),
                OverlappingWindowSet::new,
                windowFn,
                trigger,
                TimestampedEntry::fromWindowResult,
                clock::get
        );
    }
}

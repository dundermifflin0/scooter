/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.util.DelimitedParser;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.util.TupleViews;
import com.hazelcast.jet.JetEngineConfig;
import com.hazelcast.jet.Outbox;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class TextDelimited extends Scheme<JetEngineConfig, Iterator<Map.Entry>,
        Outbox, Void, StringBuilder> {

    private static final Random RANDOM = new Random();
    private static final long HEADER = 0L;
    private final DelimitedParser delimitedParser;
    private final boolean skipHeader;

    public TextDelimited(DelimitedParser delimitedParser) {
        this(Fields.ALL, delimitedParser);
    }

    public TextDelimited(Fields fields, DelimitedParser delimitedParser) {
        this(fields, false, delimitedParser);
    }

    public TextDelimited(Fields fields, boolean skipHeader, DelimitedParser delimitedParser) {
        super(fields, fields);
        this.skipHeader = skipHeader;
        this.delimitedParser = delimitedParser;

        // normalizes ALL and UNKNOWN
        // calls reset on delimitedParser
        setSourceFields(fields);
        setSinkFields(fields);
    }

    @Override
    public void setSinkFields(Fields sinkFields) {
        super.setSourceFields(sinkFields);
        super.setSinkFields(sinkFields);

        if (delimitedParser != null) {
            delimitedParser.reset(getSourceFields(), getSinkFields());
        }
    }

    @Override
    public void setSourceFields(Fields sourceFields) {
        super.setSourceFields(sourceFields);
        super.setSinkFields(sourceFields);

        if (delimitedParser != null) {
            delimitedParser.reset(getSourceFields(), getSinkFields());
        }
    }

    @Override
    public void sourceConfInit(FlowProcess<? extends JetEngineConfig> flowProcess,
                               Tap<JetEngineConfig, Iterator<Map.Entry>, Outbox> tap,
                               JetEngineConfig conf) {

    }

    @Override
    public void sinkConfInit(FlowProcess<? extends JetEngineConfig> flowProcess,
                             Tap<JetEngineConfig, Iterator<Map.Entry>, Outbox> tap,
                             JetEngineConfig conf) {
    }

    @Override
    public void sourcePrepare(FlowProcess<? extends JetEngineConfig> flowProcess,
                              SourceCall<Void, Iterator<Map.Entry>> sourceCall)
            throws IOException {
        sourceCall.getIncomingEntry().setTuple(TupleViews.createObjectArray());
        //TODO: should not create array for each call of prepare
    }

    @Override
    public boolean source(FlowProcess<? extends JetEngineConfig> flowProcess,
                          SourceCall<Void, Iterator<Map.Entry>> sourceCall) throws IOException {
        Iterator<Map.Entry> iterator = sourceCall.getInput();
        if (!iterator.hasNext()) {
            return false;
        }
        Map.Entry pair = iterator.next();
        if (skipHeader && (Long) pair.getKey() == HEADER) {
            if (!iterator.hasNext()) {
                return false;
            }
            pair = iterator.next();
        }
        Object[] split;
        //TODO: thread-safety issue in DelimitedParser
        synchronized (delimitedParser) {
            split = delimitedParser.parseLine(pair.getValue().toString());
        }
        // assumption it is better to re-use than to construct new
        Tuple tuple = sourceCall.getIncomingEntry().getTuple();
        TupleViews.reset(tuple, split);
        return true;
    }

    @Override
    public void sinkPrepare(FlowProcess<? extends JetEngineConfig> flowProcess, SinkCall<StringBuilder,
            Outbox> sinkCall) throws IOException {
        sinkCall.setContext(new StringBuilder());
    }

    @Override
    public void sink(FlowProcess<? extends
            JetEngineConfig> flowProcess, SinkCall<StringBuilder, Outbox> sinkCall) throws IOException {
        Outbox outbox = sinkCall.getOutput();
        TupleEntry outgoing = sinkCall.getOutgoingEntry();
        Iterable<String> strings = outgoing.asIterableOf(String.class);
        StringBuilder stringBuilder = sinkCall.getContext();
        delimitedParser.joinLine(strings, stringBuilder);
        outbox.add(new AbstractMap.SimpleImmutableEntry<>(nextId(), stringBuilder.toString()));
        stringBuilder.setLength(0);
    }

    protected long nextId() {
        return Math.abs(RANDOM.nextLong());
    }

}
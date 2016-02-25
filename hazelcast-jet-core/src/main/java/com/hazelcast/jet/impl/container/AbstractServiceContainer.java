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

package com.hazelcast.jet.impl.container;

import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.ContainerResponse;
import com.hazelcast.jet.api.statemachine.container.ContainerEvent;
import com.hazelcast.jet.api.statemachine.container.ContainerState;
import com.hazelcast.jet.api.statemachine.container.ContainerStateMachineFactory;
import com.hazelcast.spi.NodeEngine;

public abstract class AbstractServiceContainer
        <SI extends ContainerEvent,
                SS extends ContainerState,
                SO extends ContainerResponse> extends AbstractContainer<SI, SS, SO> {
    public AbstractServiceContainer(ContainerStateMachineFactory<SI, SS, SO> stateMachineFactory,
                                    NodeEngine nodeEngine,
                                    ApplicationContext applicationContext) {
        super(stateMachineFactory, nodeEngine, applicationContext, null);
    }
}
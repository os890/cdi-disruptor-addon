/*
 * Copyright 2026 os890
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.os890.cdi.disruptor.impl;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.AfterDeploymentValidation;
import jakarta.enterprise.inject.spi.AnnotatedMethod;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.BeforeShutdown;
import jakarta.enterprise.inject.spi.DefinitionException;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.enterprise.inject.spi.ProcessInjectionPoint;
import jakarta.enterprise.inject.spi.ProcessManagedBean;
import org.os890.cdi.disruptor.DisruptorConfig;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.ObservesDisruptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Portable extension which
 * <ul>
 * <li>registers a bean for every {@link DisruptorEvent} injection point (one per distinct set of qualifiers)</li>
 * <li>collects all observer methods with a parameter annotated with {@link ObservesDisruptor}</li>
 * <li>starts the disruptor after the deployment and stops it before the application-context gets destroyed</li>
 * </ul>
 */
public class DisruptorExtension implements Extension {
    private final List<ObserverDefinition> observerDefinitions = new ArrayList<>();
    private final Map<Set<Annotation>, Integer> channelIds = new HashMap<>();
    private final List<Set<Annotation>> channelQualifiers = new ArrayList<>();
    private final List<Set<Type>> channelTypes = new ArrayList<>();

    private volatile BeanManager beanManager;
    private volatile DisruptorDispatcher dispatcher;

    void init(@Observes BeforeBeanDiscovery beforeBeanDiscovery, BeanManager beanManager) {
        this.beanManager = beanManager;
    }

    synchronized void collectDisruptorEventInjectionPoints(@Observes ProcessInjectionPoint<?, ?> event) {
        InjectionPoint injectionPoint = event.getInjectionPoint();
        Type type = injectionPoint.getType();
        if (!isDisruptorEventType(type)) {
            return;
        }
        if (!(type instanceof ParameterizedType parameterizedType)
                || !isConcrete(parameterizedType.getActualTypeArguments()[0])) {
            event.addDefinitionError(new DefinitionException("invalid injection point " + injectionPoint
                    + " - the type parameter of " + DisruptorEvent.class.getSimpleName()
                    + " has to be a concrete type"));
            return;
        }

        Set<Annotation> qualifiers = Qualifiers.normalizeEventQualifiers(injectionPoint.getQualifiers());
        int channelId = channelIds.computeIfAbsent(qualifiers, newQualifiers -> {
            channelQualifiers.add(newQualifiers);
            channelTypes.add(new HashSet<>());
            return channelQualifiers.size() - 1;
        });
        channelTypes.get(channelId).add(type);

        // the original qualifiers are kept by the bean of the channel
        event.configureInjectionPoint().qualifiers(new DisruptorEventChannel.Literal(channelId));
    }

    synchronized void collectObserverMethods(@Observes ProcessManagedBean<?> event, BeanManager beanManager) {
        Bean<?> bean = event.getBean();
        for (AnnotatedMethod<?> method : event.getAnnotatedBeanClass().getMethods()) {
            boolean observerMethod = method.getParameters().stream()
                    .anyMatch(parameter -> parameter.isAnnotationPresent(ObservesDisruptor.class));
            if (!observerMethod) {
                continue;
            }

            try {
                DisruptorObserverMethod.validateDefinition(bean, method, beanManager);
                observerDefinitions.add(new ObserverDefinition(bean, method));
            } catch (DefinitionException e) {
                event.addDefinitionError(e);
            }
        }
    }

    synchronized void registerDisruptorEventBeans(@Observes AfterBeanDiscovery afterBeanDiscovery) {
        for (int channelId = 0; channelId < channelQualifiers.size(); channelId++) {
            Set<Annotation> qualifiers = channelQualifiers.get(channelId);
            Set<Type> types = new HashSet<>(channelTypes.get(channelId));
            types.add(Object.class);

            afterBeanDiscovery.addBean()
                    .id(DisruptorEventImpl.class.getName() + "#" + channelId)
                    .beanClass(DisruptorEventImpl.class)
                    .types(types)
                    .qualifiers(new DisruptorEventChannel.Literal(channelId))
                    .scope(Dependent.class)
                    .createWith(creationalContext -> new DisruptorEventImpl<>(this, qualifiers));
        }
    }

    synchronized void startDispatcher(@Observes AfterDeploymentValidation afterDeploymentValidation,
                                      BeanManager beanManager) {
        List<DisruptorObserverMethod> observerMethods = new ArrayList<>(observerDefinitions.size());
        boolean valid = true;
        for (ObserverDefinition definition : observerDefinitions) {
            try {
                observerMethods.add(new DisruptorObserverMethod(
                        definition.bean(), definition.method(), this, beanManager));
            } catch (RuntimeException e) {
                afterDeploymentValidation.addDeploymentProblem(e);
                valid = false;
            }
        }
        observerDefinitions.clear();

        if (valid) {
            DisruptorDispatcher newDispatcher =
                    new DisruptorDispatcher(beanManager, observerMethods, resolveConfig(beanManager));
            newDispatcher.start();
            dispatcher = newDispatcher;
        }
    }

    // stop before the application-context gets destroyed to allow application-scoped observers to process
    // pending events
    void stopDispatcher(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
        stop();
    }

    void stopDispatcher(@Observes BeforeShutdown event) {
        stop();
    }

    BeanManager beanManager() {
        return beanManager;
    }

    DisruptorDispatcher dispatcher() {
        DisruptorDispatcher result = dispatcher;
        if (result == null) {
            throw new IllegalStateException("the disruptor isn't started - events can be fired after the deployment");
        }
        return result;
    }

    private void stop() {
        DisruptorDispatcher current = dispatcher;
        if (current != null) {
            current.stop();
        }
    }

    private static DisruptorConfig resolveConfig(BeanManager beanManager) {
        Instance<DisruptorConfig> configInstance = beanManager.createInstance().select(DisruptorConfig.class);
        if (configInstance.isResolvable()) {
            return configInstance.get();
        }
        return new DisruptorConfig() {
        };
    }

    private static boolean isDisruptorEventType(Type type) {
        return type == DisruptorEvent.class || (type instanceof ParameterizedType parameterizedType
                && parameterizedType.getRawType() == DisruptorEvent.class);
    }

    private static boolean isConcrete(Type type) {
        if (type instanceof Class<?>) {
            return true;
        }
        if (type instanceof ParameterizedType parameterizedType) {
            for (Type typeArgument : parameterizedType.getActualTypeArguments()) {
                if (!isConcrete(typeArgument)) {
                    return false;
                }
            }
            return true;
        }
        if (type instanceof GenericArrayType genericArrayType) {
            return isConcrete(genericArrayType.getGenericComponentType());
        }
        // type-variables and wildcards
        return false;
    }

    private record ObserverDefinition(Bean<?> bean, AnnotatedMethod<?> method) {
    }
}

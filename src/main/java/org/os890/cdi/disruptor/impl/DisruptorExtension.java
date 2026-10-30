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
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.AfterDeploymentValidation;
import jakarta.enterprise.inject.spi.AnnotatedMethod;
import jakarta.enterprise.inject.spi.AnnotatedParameter;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.BeforeShutdown;
import jakarta.enterprise.inject.spi.DefinitionException;
import jakarta.enterprise.inject.spi.DeploymentException;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.enterprise.inject.spi.ProcessInjectionPoint;
import jakarta.enterprise.inject.spi.ProcessManagedBean;
import org.os890.cdi.disruptor.DisruptorConfig;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.ObservesDisruptor;
import org.os890.cdi.disruptor.RingBuffer;
import org.os890.cdi.disruptor.RingBufferSettings;

import java.lang.annotation.Annotation;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Member;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Portable extension which
 * <ul>
 * <li>registers a bean for every {@link DisruptorEvent} injection point
 *     (one per ring-buffer and distinct set of qualifiers)</li>
 * <li>collects all observer methods with a parameter annotated with {@link ObservesDisruptor}</li>
 * <li>collects the configuration of all ring-buffers (see {@link RingBuffer})</li>
 * <li>starts one disruptor per ring-buffer after the deployment and stops them before the
 *     application-context gets destroyed</li>
 * </ul>
 */
public class DisruptorExtension implements Extension {
    private final List<ObserverDefinition> observerDefinitions = new ArrayList<>();
    private final Map<Channel, Integer> channelIds = new HashMap<>();
    private final List<Channel> channels = new ArrayList<>();
    private final List<Set<Type>> channelTypes = new ArrayList<>();
    private final Map<String, RingBufferDeclaration> ringBufferDeclarations = new HashMap<>();

    private volatile BeanManager beanManager;
    private volatile Map<String, DisruptorDispatcher> dispatchers;
    // events for ring-buffers which are only referenced at runtime (via DisruptorEvent#select) have no observers
    private volatile DisruptorDispatcher undeclaredRingBufferDispatcher;

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
            event.addDefinitionError(new DefinitionException("invalid injection point " + describe(injectionPoint)
                    + " - the type parameter of " + DisruptorEvent.class.getSimpleName()
                    + " has to be a concrete type"));
            return;
        }

        Set<Annotation> qualifiers = injectionPoint.getQualifiers();
        try {
            declareRingBuffer(qualifiers, describe(injectionPoint));
        } catch (DefinitionException e) {
            event.addDefinitionError(e);
            return;
        }

        Channel channel = new Channel(Qualifiers.ringBufferName(qualifiers),
                Qualifiers.normalizeEventQualifiers(qualifiers));
        int channelId = channelIds.computeIfAbsent(channel, newChannel -> {
            channels.add(newChannel);
            channelTypes.add(new HashSet<>());
            return channels.size() - 1;
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
                for (AnnotatedParameter<?> parameter : method.getParameters()) {
                    if (parameter.isAnnotationPresent(ObservesDisruptor.class)
                            || isDisruptorEventType(parameter.getBaseType())) {
                        declareRingBuffer(Qualifiers.qualifiersOf(parameter, beanManager),
                                describe(method.getJavaMember()));
                    }
                }
                observerDefinitions.add(new ObserverDefinition(bean, method));
            } catch (DefinitionException e) {
                event.addDefinitionError(e);
            }
        }
    }

    synchronized void registerDisruptorEventBeans(@Observes AfterBeanDiscovery afterBeanDiscovery) {
        for (int channelId = 0; channelId < channels.size(); channelId++) {
            Channel channel = channels.get(channelId);
            Set<Type> types = new HashSet<>(channelTypes.get(channelId));
            types.add(Object.class);

            afterBeanDiscovery.addBean()
                    .id(DisruptorEventImpl.class.getName() + "#" + channelId)
                    .beanClass(DisruptorEventImpl.class)
                    .types(types)
                    .qualifiers(new DisruptorEventChannel.Literal(channelId))
                    .scope(Dependent.class)
                    .createWith(creationalContext ->
                        new DisruptorEventImpl<>(this, channel.ringBufferName(), channel.qualifiers()));
        }
    }

    synchronized void startDispatchers(@Observes AfterDeploymentValidation afterDeploymentValidation,
                                       BeanManager beanManager) {
        boolean valid = true;

        Map<String, List<DisruptorObserverMethod>> observerMethods = new HashMap<>();
        for (ObserverDefinition definition : observerDefinitions) {
            try {
                DisruptorObserverMethod observerMethod =
                        new DisruptorObserverMethod(definition.bean(), definition.method(), this, beanManager);
                observerMethods.computeIfAbsent(observerMethod.ringBufferName(), name -> new ArrayList<>())
                        .add(observerMethod);
            } catch (RuntimeException e) {
                afterDeploymentValidation.addDeploymentProblem(e);
                valid = false;
            }
        }
        observerDefinitions.clear();

        ringBufferDeclarations.computeIfAbsent(RingBuffer.DEFAULT, RingBufferDeclaration::new);
        List<DisruptorConfig> configs = resolveConfigs(beanManager);
        Map<String, DisruptorDispatcher> newDispatchers = new HashMap<>();
        for (RingBufferDeclaration declaration : ringBufferDeclarations.values()) {
            String name = declaration.name();
            try {
                newDispatchers.put(name, new DisruptorDispatcher(beanManager,
                        observerMethods.getOrDefault(name, List.of()), resolveSettings(declaration, configs)));
            } catch (RuntimeException e) {
                afterDeploymentValidation.addDeploymentProblem(new DeploymentException("invalid configuration of the"
                        + " ring-buffer '" + name + "' declared at " + declaration.declaredAt(), e));
                valid = false;
            }
        }

        if (valid) {
            newDispatchers.values().forEach(DisruptorDispatcher::start);
            DisruptorDispatcher emptyDispatcher =
                    new DisruptorDispatcher(beanManager, List.of(), new RingBufferSettings("undeclared"));
            emptyDispatcher.start();
            undeclaredRingBufferDispatcher = emptyDispatcher;
            dispatchers = Map.copyOf(newDispatchers);
        }
    }

    // stop before the application-context gets destroyed to allow application-scoped observers to process
    // pending events
    void stopDispatchers(@Observes @BeforeDestroyed(ApplicationScoped.class) Object event) {
        stop();
    }

    void stopDispatchers(@Observes BeforeShutdown event) {
        stop();
    }

    BeanManager beanManager() {
        return beanManager;
    }

    DisruptorDispatcher dispatcher(String ringBufferName) {
        Map<String, DisruptorDispatcher> currentDispatchers = dispatchers;
        if (currentDispatchers == null) {
            throw new IllegalStateException("the disruptor isn't started - events can be fired after the deployment");
        }
        DisruptorDispatcher result = currentDispatchers.get(ringBufferName);
        return result != null ? result : undeclaredRingBufferDispatcher;
    }

    private void stop() {
        Map<String, DisruptorDispatcher> currentDispatchers = dispatchers;
        if (currentDispatchers != null) {
            currentDispatchers.values().forEach(DisruptorDispatcher::stop);
            undeclaredRingBufferDispatcher.stop();
        }
    }

    private void declareRingBuffer(Collection<Annotation> qualifiers, String source) {
        RingBuffer ringBuffer = Qualifiers.ringBufferQualifier(qualifiers);
        String name = Qualifiers.ringBufferName(qualifiers);
        if (name.isBlank()) {
            throw new DefinitionException("the name of @" + RingBuffer.class.getSimpleName()
                    + " must not be blank - declared at " + source);
        }

        RingBufferDeclaration declaration = ringBufferDeclarations.computeIfAbsent(name, RingBufferDeclaration::new);
        if (ringBuffer != null) {
            declaration.merge(ringBuffer, source);
        }
    }

    // later entries override former ones - see RingBuffer
    private static RingBufferSettings resolveSettings(RingBufferDeclaration declaration,
                                                      List<DisruptorConfig> configs) {
        RingBufferSettings settings = new RingBufferSettings(declaration.name());
        declaration.applyTo(settings);
        MicroProfileConfigSupport.applyTo(settings);
        for (DisruptorConfig config : configs) {
            config.configure(settings);
        }
        return settings;
    }

    private static List<DisruptorConfig> resolveConfigs(BeanManager beanManager) {
        List<DisruptorConfig> result = new ArrayList<>();
        for (DisruptorConfig config : beanManager.createInstance().select(DisruptorConfig.class)) {
            result.add(config);
        }
        return result;
    }

    private static String describe(InjectionPoint injectionPoint) {
        return describe(injectionPoint.getMember());
    }

    private static String describe(Member member) {
        return member.getDeclaringClass().getName() + "#" + member.getName();
    }

    static boolean isDisruptorEventType(Type type) {
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

    private record Channel(String ringBufferName, Set<Annotation> qualifiers) {
    }
}

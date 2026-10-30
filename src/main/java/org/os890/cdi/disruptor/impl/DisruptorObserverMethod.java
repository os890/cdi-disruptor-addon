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

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.spi.AnnotatedMethod;
import jakarta.enterprise.inject.spi.AnnotatedParameter;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.DefinitionException;
import jakarta.enterprise.inject.spi.InjectionPoint;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.ObservesDisruptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Runtime representation of a method with a parameter annotated with {@link ObservesDisruptor}.
 */
final class DisruptorObserverMethod {
    private static final Map<Class<?>, Class<?>> PRIMITIVE_WRAPPERS = Map.of(
            boolean.class, Boolean.class, byte.class, Byte.class, char.class, Character.class,
            short.class, Short.class, int.class, Integer.class, long.class, Long.class,
            float.class, Float.class, double.class, Double.class);

    private final BeanManager beanManager;
    private final Bean<?> bean;
    private final Method method;
    private final boolean staticMethod;
    private final boolean dependentBean;
    private final Type beanType;
    private final int eventParameterPosition;
    private final Class<?> observedType;
    private final Set<Annotation> observedQualifiers;
    private final InjectionPoint[] injectionPoints;
    private final DisruptorEvent<?>[] disruptorEventArguments;

    private volatile Object contextualReference;

    DisruptorObserverMethod(Bean<?> bean,
                            AnnotatedMethod<?> annotatedMethod,
                            DisruptorExtension extension,
                            BeanManager beanManager) {
        this.beanManager = beanManager;
        this.bean = bean;
        this.method = annotatedMethod.getJavaMember();
        this.method.trySetAccessible();
        this.staticMethod = Modifier.isStatic(method.getModifiers());
        this.dependentBean = Dependent.class.equals(bean.getScope());
        this.beanType = bean.getTypes().contains(bean.getBeanClass()) ? bean.getBeanClass() : Object.class;

        List<? extends AnnotatedParameter<?>> parameters = annotatedMethod.getParameters();
        this.injectionPoints = new InjectionPoint[parameters.size()];
        this.disruptorEventArguments = new DisruptorEvent<?>[parameters.size()];

        int eventPosition = -1;
        for (AnnotatedParameter<?> parameter : parameters) {
            int position = parameter.getPosition();
            if (parameter.isAnnotationPresent(ObservesDisruptor.class)) {
                eventPosition = position;
            } else if (rawType(parameter.getBaseType()) == DisruptorEvent.class) {
                // injection points of observer methods aren't processed by the container - see ProcessInjectionPoint
                disruptorEventArguments[position] = new DisruptorEventImpl<>(extension,
                        Qualifiers.normalizeEventQualifiers(Qualifiers.qualifiersOf(parameter, beanManager)));
            } else {
                // BeanManager#createInjectionPoint isn't used, because some containers (e.g. OWB) would fire
                // ProcessInjectionPoint after the deployment
                InjectionPoint injectionPoint = new ObserverParameterInjectionPoint(
                    bean, parameter, Qualifiers.qualifiersOf(parameter, beanManager));
                beanManager.validate(injectionPoint);
                injectionPoints[position] = injectionPoint;
            }
        }
        this.eventParameterPosition = eventPosition;

        AnnotatedParameter<?> eventParameter = parameters.get(eventPosition);
        this.observedType = boxed(rawType(eventParameter.getBaseType()));
        this.observedQualifiers = Qualifiers.qualifiersOf(eventParameter, beanManager).stream()
                .filter(qualifier -> qualifier.annotationType() != Any.class)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Validates the definition of an observer method (before the bean-manager is fully available).
     *
     * @param bean            bean which declares the method
     * @param annotatedMethod method with at least one parameter annotated with {@link ObservesDisruptor}
     * @param beanManager     current bean-manager
     * @throws DefinitionException if the definition is invalid
     */
    static void validateDefinition(Bean<?> bean, AnnotatedMethod<?> annotatedMethod, BeanManager beanManager) {
        Method javaMethod = annotatedMethod.getJavaMember();
        String methodName = javaMethod.getDeclaringClass().getName() + "#" + javaMethod.getName();

        List<? extends AnnotatedParameter<?>> eventParameters = annotatedMethod.getParameters().stream()
                .filter(parameter -> parameter.isAnnotationPresent(ObservesDisruptor.class))
                .collect(Collectors.toList());

        if (eventParameters.size() > 1) {
            throw new DefinitionException(methodName + " has more than one parameter annotated with @"
                    + ObservesDisruptor.class.getSimpleName());
        }
        if (rawType(eventParameters.get(0).getBaseType()) == null) {
            throw new DefinitionException("the type of the parameter annotated with @"
                    + ObservesDisruptor.class.getSimpleName() + " in " + methodName
                    + " has to be a class or a parameterized type, but it is "
                    + eventParameters.get(0).getBaseType().getTypeName());
        }
        if (Modifier.isPrivate(javaMethod.getModifiers()) && !Modifier.isStatic(javaMethod.getModifiers())
                && beanManager.isNormalScope(bean.getScope())) {
            throw new DefinitionException(methodName + " is private, which isn't supported for observer methods"
                    + " of normal-scoped beans");
        }
    }

    boolean observes(Class<?> eventType, Set<Annotation> eventQualifiers) {
        if (!observedType.isAssignableFrom(eventType)) {
            return false;
        }
        for (Annotation observedQualifier : observedQualifiers) {
            if (observedQualifier.annotationType() == Default.class) {
                if (!eventQualifiers.isEmpty()) {
                    return false;
                }
            } else if (eventQualifiers.stream()
                    .noneMatch(qualifier -> beanManager.areQualifiersEquivalent(observedQualifier, qualifier))) {
                return false;
            }
        }
        return true;
    }

    void notify(Object event) throws Exception {
        CreationalContext<?> argumentsContext = beanManager.createCreationalContext(null);
        try {
            Object[] arguments = new Object[injectionPoints.length];
            for (int i = 0; i < arguments.length; i++) {
                if (i == eventParameterPosition) {
                    arguments[i] = event;
                } else if (disruptorEventArguments[i] != null) {
                    arguments[i] = disruptorEventArguments[i];
                } else {
                    arguments[i] = beanManager.getInjectableReference(injectionPoints[i], argumentsContext);
                }
            }

            if (staticMethod) {
                invoke(null, arguments);
            } else if (dependentBean) {
                invokeOnDependentInstance(bean, arguments);
            } else {
                invoke(contextualReference(), arguments);
            }
        } finally {
            argumentsContext.release();
        }
    }

    private <T> void invokeOnDependentInstance(Bean<T> dependent, Object[] arguments) throws Exception {
        CreationalContext<T> creationalContext = beanManager.createCreationalContext(dependent);
        T instance = dependent.create(creationalContext);
        try {
            invoke(instance, arguments);
        } finally {
            dependent.destroy(instance, creationalContext);
        }
    }

    private Object contextualReference() {
        Object result = contextualReference;
        if (result == null) {
            // client-proxy (or the instance itself for pseudo-scopes) - safe to cache
            result = beanManager.getReference(bean, beanType, beanManager.createCreationalContext(bean));
            contextualReference = result;
        }
        return result;
    }

    private void invoke(Object target, Object[] arguments) throws Exception {
        try {
            method.invoke(target, arguments);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    private static Class<?> rawType(Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof ParameterizedType parameterizedType) {
            return (Class<?>) parameterizedType.getRawType();
        }
        return null;
    }

    private static Class<?> boxed(Class<?> type) {
        return PRIMITIVE_WRAPPERS.getOrDefault(type, type);
    }

    @Override
    public String toString() {
        return method.getDeclaringClass().getName() + "#" + method.getName();
    }
}

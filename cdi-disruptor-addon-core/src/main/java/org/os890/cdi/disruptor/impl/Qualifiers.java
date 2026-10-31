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

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.spi.Annotated;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.util.Nonbinding;
import org.os890.cdi.disruptor.RingBuffer;

import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Internal API - not intended to be used by applications.
 */
public final class Qualifiers {
    // members of annotation types without @Nonbinding
    private static final ClassValue<List<Method>> BINDING_MEMBERS = new ClassValue<>() {
        @Override
        protected List<Method> computeValue(Class<?> annotationType) {
            return Arrays.stream(annotationType.getDeclaredMethods())
                .filter(member -> !member.isAnnotationPresent(Nonbinding.class))
                .peek(Method::trySetAccessible)
                .toList();
        }
    };

    private Qualifiers() {
        // utility class
    }

    /**
     * @param qualifiers qualifiers of an injection point or a fired event
     * @return the given qualifiers without {@link Any}, {@link Default} and {@link RingBuffer} -
     *         an empty set represents {@link Default}
     */
    public static Set<Annotation> normalizeEventQualifiers(Collection<Annotation> qualifiers) {
        Set<Annotation> result = new HashSet<>();
        for (Annotation qualifier : qualifiers) {
            if (!isIgnoredForMatching(qualifier)) {
                result.add(qualifier);
            }
        }
        return Collections.unmodifiableSet(result);
    }

    /**
     * @param existingQualifiers   normalized qualifiers
     * @param additionalQualifiers qualifiers to add
     * @param isQualifier          checks if an annotation is a qualifier
     * @return the normalized union of both
     * @throws IllegalArgumentException for annotations which aren't qualifiers or for duplicated qualifiers
     */
    static Set<Annotation> merge(Set<Annotation> existingQualifiers,
                                 Annotation[] additionalQualifiers,
                                 Predicate<Class<? extends Annotation>> isQualifier) {
        Set<Annotation> result = new HashSet<>(existingQualifiers);
        for (Annotation qualifier : additionalQualifiers) {
            Class<? extends Annotation> qualifierType = qualifier.annotationType();
            if (!isQualifier.test(qualifierType)) {
                throw new IllegalArgumentException(qualifierType.getName() + " isn't a qualifier");
            }
            if (isIgnoredForMatching(qualifier)) {
                continue;
            }
            if (!qualifierType.isAnnotationPresent(Repeatable.class)
                    && result.stream().anyMatch(existing -> existing.annotationType() == qualifierType)) {
                throw new IllegalArgumentException("duplicated qualifier " + qualifierType.getName());
            }
            result.add(qualifier);
        }
        return Collections.unmodifiableSet(result);
    }

    /**
     * @param annotated   annotated element
     * @param beanManager current bean-manager
     * @return all qualifiers of the given element
     */
    public static Set<Annotation> qualifiersOf(Annotated annotated, BeanManager beanManager) {
        Set<Annotation> result = new HashSet<>();
        for (Annotation annotation : annotated.getAnnotations()) {
            if (beanManager.isQualifier(annotation.annotationType())) {
                result.add(annotation);
            }
        }
        return result;
    }

    /**
     * @param annotations qualifiers
     * @return the {@link RingBuffer} qualifier or null
     */
    public static RingBuffer ringBufferQualifier(Collection<Annotation> annotations) {
        for (Annotation annotation : annotations) {
            if (annotation instanceof RingBuffer ringBuffer) {
                return ringBuffer;
            }
        }
        return null;
    }

    /**
     * @param annotations qualifiers
     * @return the name of the selected ring-buffer
     */
    public static String ringBufferName(Collection<Annotation> annotations) {
        RingBuffer ringBuffer = ringBufferQualifier(annotations);
        return ringBuffer == null ? RingBuffer.DEFAULT : ringBuffer.name();
    }

    /**
     * @param observedType       observed (raw) type of the observer method
     * @param observedQualifiers qualifiers of the observer method (without {@link Any} and {@link RingBuffer})
     * @param eventType          runtime type of the event
     * @param eventQualifiers    normalized qualifiers of the event
     * @param equivalence        checks if two qualifiers are equivalent
     * @return true if the observer method observes the event
     */
    public static boolean observes(Class<?> observedType,
                                   Set<Annotation> observedQualifiers,
                                   Class<?> eventType,
                                   Set<Annotation> eventQualifiers,
                                   BiPredicate<Annotation, Annotation> equivalence) {
        if (!observedType.isAssignableFrom(eventType)) {
            return false;
        }
        for (Annotation observedQualifier : observedQualifiers) {
            if (observedQualifier.annotationType() == Default.class) {
                if (!eventQualifiers.isEmpty()) {
                    return false;
                }
            } else if (eventQualifiers.stream()
                    .noneMatch(qualifier -> equivalence.test(observedQualifier, qualifier))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares qualifiers like a CDI container: members annotated with {@link Nonbinding} get ignored.
     *
     * @param qualifier      a qualifier
     * @param otherQualifier another qualifier
     * @return true if both qualifiers are equivalent
     */
    public static boolean areEquivalent(Annotation qualifier, Annotation otherQualifier) {
        Class<? extends Annotation> annotationType = qualifier.annotationType();
        if (annotationType != otherQualifier.annotationType()) {
            return false;
        }
        for (Method member : BINDING_MEMBERS.get(annotationType)) {
            try {
                if (!Objects.deepEquals(member.invoke(qualifier), member.invoke(otherQualifier))) {
                    return false;
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException("failed to read " + member + " of " + qualifier, e);
            }
        }
        return true;
    }

    // @RingBuffer selects the ring-buffer and isn't used for matching observers
    private static boolean isIgnoredForMatching(Annotation qualifier) {
        Class<? extends Annotation> qualifierType = qualifier.annotationType();
        return qualifierType == Any.class || qualifierType == Default.class || qualifierType == RingBuffer.class;
    }
}

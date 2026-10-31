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
package org.os890.cdi.disruptor.lite;

import com.lmax.disruptor.WaitStrategy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.context.control.ActivateRequestContext;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.enterprise.event.Shutdown;
import jakarta.enterprise.event.Startup;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.build.compatible.spi.BeanInfo;
import jakarta.enterprise.inject.build.compatible.spi.BuildCompatibleExtension;
import jakarta.enterprise.inject.build.compatible.spi.ClassConfig;
import jakarta.enterprise.inject.build.compatible.spi.DeclarationConfig;
import jakarta.enterprise.inject.build.compatible.spi.Discovery;
import jakarta.enterprise.inject.build.compatible.spi.Enhancement;
import jakarta.enterprise.inject.build.compatible.spi.FieldConfig;
import jakarta.enterprise.inject.build.compatible.spi.InvokerFactory;
import jakarta.enterprise.inject.build.compatible.spi.InvokerInfo;
import jakarta.enterprise.inject.build.compatible.spi.Messages;
import jakarta.enterprise.inject.build.compatible.spi.MethodConfig;
import jakarta.enterprise.inject.build.compatible.spi.ParameterConfig;
import jakarta.enterprise.inject.build.compatible.spi.Registration;
import jakarta.enterprise.inject.build.compatible.spi.ScannedClasses;
import jakarta.enterprise.inject.build.compatible.spi.Synthesis;
import jakarta.enterprise.inject.build.compatible.spi.SyntheticBeanBuilder;
import jakarta.enterprise.inject.build.compatible.spi.SyntheticComponents;
import jakarta.enterprise.invoke.InvokerBuilder;
import jakarta.enterprise.lang.model.AnnotationInfo;
import jakarta.enterprise.lang.model.AnnotationMember;
import jakarta.enterprise.lang.model.AnnotationTarget;
import jakarta.enterprise.lang.model.declarations.ClassInfo;
import jakarta.enterprise.lang.model.declarations.FieldInfo;
import jakarta.enterprise.lang.model.declarations.MethodInfo;
import jakarta.enterprise.lang.model.declarations.ParameterInfo;
import jakarta.enterprise.lang.model.types.Type;
import jakarta.inject.Inject;
import jakarta.inject.Qualifier;
import org.os890.cdi.disruptor.CircuitBreaker;
import org.os890.cdi.disruptor.DisruptorEvent;
import org.os890.cdi.disruptor.ObservesDisruptor;
import org.os890.cdi.disruptor.RingBuffer;
import org.os890.cdi.disruptor.impl.DisruptorEventChannel;
import org.os890.cdi.disruptor.impl.DisruptorEventImpl;
import org.os890.cdi.disruptor.impl.RingBufferDeclaration;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Build-compatible extension (CDI Lite) - the counterpart of the portable extension for containers like Quarkus:
 * <ul>
 * <li>{@link Discovery}: registers the internal channel qualifier</li>
 * <li>{@link Enhancement}: replaces the qualifiers of {@link DisruptorEvent} injection points with an internal
 *     channel qualifier (one channel per ring-buffer, qualifiers and circuit-breaker), collects the
 *     configuration of the ring-buffers and adds {@link ActivateRequestContext} to observer methods which
 *     require a request-context</li>
 * <li>{@link Registration}: collects the observer methods and creates an invoker for each of them</li>
 * <li>{@link Synthesis}: adds a bean per channel and observers which start the runtime with the collected
 *     metadata ({@link Startup}) and stop it ({@link Shutdown})</li>
 * </ul>
 */
public class DisruptorBuildCompatibleExtension implements BuildCompatibleExtension {
    private static final Map<String, String> PRIMITIVE_WRAPPERS = Map.of(
        "boolean", Boolean.class.getName(), "byte", Byte.class.getName(), "char", Character.class.getName(),
        "short", Short.class.getName(), "int", Integer.class.getName(), "long", Long.class.getName(),
        "float", Float.class.getName(), "double", Double.class.getName());

    private final Map<String, Channel> channels = new LinkedHashMap<>();
    private final Map<String, RingBufferDeclaration> ringBufferDeclarations = new LinkedHashMap<>();
    private final List<ObserverDefinition> observerDefinitions = new ArrayList<>();

    @Discovery
    public void registerQualifiers(ScannedClasses scannedClasses) {
        scannedClasses.add(DisruptorEventChannel.class.getName());
        scannedClasses.add(RingBuffer.class.getName());
    }

    @Enhancement(types = Object.class, withSubtypes = true)
    public void processInjectionPoints(ClassConfig clazz, Messages messages) {
        for (FieldConfig field : clazz.fields()) {
            FieldInfo info = field.info();
            if (info.hasAnnotation(Inject.class)) {
                processInjectionPoint(field, info.type(), info.annotations(), info, describe(info), messages);
            }
        }

        for (MethodConfig method : concat(clazz.constructors(), clazz.methods())) {
            MethodInfo info = method.info();
            boolean observerMethod = hasParameterWith(info, ObservesDisruptor.class);
            if (observerMethod && isRequestContextRequired(info)) {
                if (info.isStatic()) {
                    messages.error("requestContext = true isn't supported for static observer methods", info);
                } else {
                    // the invoker calls the method on the contextual instance - the interceptor activates the context
                    method.addAnnotation(ActivateRequestContext.class);
                }
            }
            if (!observerMethod && info.hasAnnotation(CircuitBreaker.class)) {
                messages.error("@" + CircuitBreaker.class.getSimpleName() + " is only supported for observer methods"
                    + " with a parameter annotated with @" + ObservesDisruptor.class.getSimpleName(), info);
            }
            if (!observerMethod && !info.hasAnnotation(Inject.class) && !info.hasAnnotation(Produces.class)
                    && !hasParameterWith(info, Observes.class) && !hasParameterWith(info, ObservesAsync.class)
                    && !hasParameterWith(info, Disposes.class)) {
                continue;
            }

            for (ParameterConfig parameter : method.parameters()) {
                ParameterInfo parameterInfo = parameter.info();
                if (parameterInfo.hasAnnotation(ObservesDisruptor.class)) {
                    if (parameterInfo.hasAnnotation(CircuitBreaker.class)) {
                        messages.error("@" + CircuitBreaker.class.getSimpleName() + " isn't supported for the event"
                            + " parameter - annotate the observer method (failing observer) or "
                            + DisruptorEvent.class.getSimpleName() + " injection points (producer)", parameterInfo);
                    }
                    continue;
                }
                processInjectionPoint(parameter, parameterInfo.type(), parameterInfo.annotations(), parameterInfo,
                    describe(info), messages);
            }
        }
    }

    @Registration(types = Object.class)
    public void collectObserverMethods(BeanInfo bean, InvokerFactory invokers, Messages messages) {
        if (!bean.isClassBean()) {
            return;
        }

        Set<String> processedMethods = new HashSet<>();
        for (ClassInfo clazz = bean.declaringClass();
                clazz != null && !Object.class.getName().equals(clazz.name());
                clazz = clazz.superClassDeclaration()) {
            for (MethodInfo method : clazz.methods()) {
                if (hasParameterWith(method, ObservesDisruptor.class) && processedMethods.add(signature(method))) {
                    collectObserverMethod(bean, method, invokers, messages);
                }
            }
        }
    }

    @Synthesis
    public void addComponents(SyntheticComponents components) {
        for (Channel channel : channels.values()) {
            @SuppressWarnings("rawtypes")
            SyntheticBeanBuilder<DisruptorEventImpl> bean = components.addBean(DisruptorEventImpl.class);
            channel.types().values().forEach(bean::type);
            bean.qualifier(DisruptorEventChannel.Literal.of(channel.id()))
                .scope(Dependent.class)
                .withParam(DisruptorEventCreator.RING_BUFFER_NAME, channel.ringBufferName())
                .withParam(DisruptorEventCreator.QUALIFIERS, channel.qualifiers().toArray(new AnnotationInfo[0]))
                .withParam(DisruptorEventCreator.CIRCUIT_BREAKER, channel.circuitBreaker())
                .createWith(DisruptorEventCreator.class);
        }

        ringBufferDeclarations.computeIfAbsent(RingBuffer.DEFAULT, RingBufferDeclaration::new);
        List<RingBufferDeclaration> declarations = new ArrayList<>(ringBufferDeclarations.values());

        // the runtime gets started before other observers of the startup event and stopped after them
        components.addObserver(Startup.class)
            .priority(Integer.MIN_VALUE)
            .withParam(StartupObserver.INVOKERS,
                observerDefinitions.stream().map(ObserverDefinition::invoker).toArray(InvokerInfo[]::new))
            .withParam(StartupObserver.OBSERVED_TYPES,
                observerDefinitions.stream().map(ObserverDefinition::observedType).toArray(String[]::new))
            .withParam(StartupObserver.QUALIFIERS, observerDefinitions.stream()
                .flatMap(definition -> definition.qualifiers().stream()).toArray(AnnotationInfo[]::new))
            .withParam(StartupObserver.QUALIFIER_COUNTS,
                observerDefinitions.stream().mapToInt(definition -> definition.qualifiers().size()).toArray())
            .withParam(StartupObserver.RING_BUFFER_NAMES,
                observerDefinitions.stream().map(ObserverDefinition::ringBufferName).toArray(String[]::new))
            .withParam(StartupObserver.CIRCUIT_BREAKER, toBooleanArray(observerDefinitions.stream()
                .map(ObserverDefinition::circuitBreaker).collect(Collectors.toList())))
            .withParam(StartupObserver.EVENT_PARAMETER_POSITIONS,
                observerDefinitions.stream().mapToInt(ObserverDefinition::eventParameterPosition).toArray())
            .withParam(StartupObserver.PARAMETER_COUNTS,
                observerDefinitions.stream().mapToInt(ObserverDefinition::parameterCount).toArray())
            .withParam(StartupObserver.DESCRIPTIONS,
                observerDefinitions.stream().map(ObserverDefinition::description).toArray(String[]::new))
            .withParam(StartupObserver.DECLARED_RING_BUFFERS,
                declarations.stream().map(RingBufferDeclaration::name).toArray(String[]::new))
            .withParam(StartupObserver.DECLARED_SIZES,
                declarations.stream().mapToInt(RingBufferDeclaration::size).toArray())
            .withParam(StartupObserver.DECLARED_WAIT_STRATEGIES,
                declarations.stream().map(RingBufferDeclaration::waitStrategyName).toArray(String[]::new))
            .withParam(StartupObserver.DECLARED_AT, declarations.stream()
                .map(declaration -> String.join(", ", declaration.declaredAt())).toArray(String[]::new))
            .observeWith(StartupObserver.class);

        components.addObserver(Shutdown.class)
            .priority(Integer.MAX_VALUE)
            .observeWith(ShutdownObserver.class);
    }

    private void processInjectionPoint(DeclarationConfig config,
                                       Type type,
                                       Collection<AnnotationInfo> annotations,
                                       AnnotationTarget target,
                                       String source,
                                       Messages messages) {
        boolean circuitBreaker = annotations.stream()
            .anyMatch(annotation -> CircuitBreaker.class.getName().equals(annotation.name()));
        if (!DisruptorEvent.class.getName().equals(rawTypeName(type))) {
            if (circuitBreaker) {
                messages.error("@" + CircuitBreaker.class.getSimpleName() + " is only supported for "
                    + DisruptorEvent.class.getSimpleName() + " injection points", target);
            }
            return;
        }
        if (!type.isParameterizedType() || !isConcrete(type.asParameterizedType().typeArguments().get(0))) {
            messages.error("the type parameter of " + DisruptorEvent.class.getSimpleName()
                + " has to be a concrete type", target);
            return;
        }

        List<AnnotationInfo> qualifiers = annotations.stream().filter(this::isQualifier).collect(Collectors.toList());
        String ringBufferName;
        try {
            ringBufferName = declareRingBuffer(qualifiers, source);
        } catch (IllegalArgumentException e) {
            messages.error(e.getMessage(), target);
            return;
        }

        List<AnnotationInfo> eventQualifiers = qualifiers.stream()
            .filter(qualifier -> !isAnyDefaultOrRingBuffer(qualifier))
            .collect(Collectors.toList());
        String key = ringBufferName + "|" + circuitBreaker + "|" + eventQualifiers.stream()
            .map(DisruptorBuildCompatibleExtension::canonical).sorted().collect(Collectors.joining(","));
        Channel channel = channels.computeIfAbsent(key, newKey ->
            new Channel(channels.size(), ringBufferName, eventQualifiers, circuitBreaker, new LinkedHashMap<>()));
        channel.types().putIfAbsent(canonical(type), type);

        // the original qualifiers are kept by the bean of the channel
        config.removeAnnotation(annotation -> isQualifier((AnnotationInfo) annotation));
        config.addAnnotation(DisruptorEventChannel.Literal.of(channel.id()));
    }

    private void collectObserverMethod(BeanInfo bean, MethodInfo method, InvokerFactory invokers, Messages messages) {
        List<ParameterInfo> parameters = method.parameters();
        List<ParameterInfo> eventParameters = parameters.stream()
            .filter(parameter -> parameter.hasAnnotation(ObservesDisruptor.class))
            .collect(Collectors.toList());
        if (eventParameters.size() > 1) {
            messages.error("more than one parameter annotated with @" + ObservesDisruptor.class.getSimpleName(),
                method);
            return;
        }
        if (Modifier.isPrivate(method.modifiers())) {
            messages.error("private observer methods aren't supported", method);
            return;
        }

        ParameterInfo eventParameter = eventParameters.get(0);
        String observedType = observedTypeName(eventParameter.type());
        if (observedType == null) {
            messages.error("the type of the parameter annotated with @" + ObservesDisruptor.class.getSimpleName()
                + " has to be a class, a parameterized type or a primitive type", eventParameter);
            return;
        }

        List<AnnotationInfo> qualifiers = eventParameter.annotations().stream()
            .filter(this::isQualifier)
            .collect(Collectors.toList());
        String ringBufferName;
        try {
            ringBufferName = declareRingBuffer(qualifiers, describe(method));
        } catch (IllegalArgumentException e) {
            messages.error(e.getMessage(), eventParameter);
            return;
        }

        int eventParameterPosition = parameters.indexOf(eventParameter);
        InvokerBuilder<InvokerInfo> invokerBuilder = invokers.createInvoker(bean, method);
        if (!method.isStatic()) {
            invokerBuilder.withInstanceLookup();
        }
        for (int position = 0; position < parameters.size(); position++) {
            if (position != eventParameterPosition) {
                invokerBuilder.withArgumentLookup(position);
            }
        }

        observerDefinitions.add(new ObserverDefinition(invokerBuilder.build(), observedType,
            qualifiers.stream()
                .filter(qualifier -> !Any.class.getName().equals(qualifier.name())
                    && !RingBuffer.class.getName().equals(qualifier.name()))
                .collect(Collectors.toList()),
            ringBufferName, method.hasAnnotation(CircuitBreaker.class), eventParameterPosition, parameters.size(),
            describe(method)));
    }

    private String declareRingBuffer(List<AnnotationInfo> qualifiers, String source) {
        AnnotationInfo ringBuffer = qualifiers.stream()
            .filter(qualifier -> RingBuffer.class.getName().equals(qualifier.name()))
            .findFirst()
            .orElse(null);
        if (ringBuffer == null) {
            ringBufferDeclarations.computeIfAbsent(RingBuffer.DEFAULT, RingBufferDeclaration::new);
            return RingBuffer.DEFAULT;
        }

        AnnotationMember name = ringBuffer.member("name");
        String ringBufferName = name != null ? name.asString() : RingBuffer.DEFAULT;
        if (ringBufferName.isBlank()) {
            throw new IllegalArgumentException("the name of @" + RingBuffer.class.getSimpleName()
                + " must not be blank - declared at " + source);
        }
        AnnotationMember size = ringBuffer.member("size");
        AnnotationMember waitStrategy = ringBuffer.member("waitStrategy");
        ringBufferDeclarations.computeIfAbsent(ringBufferName, RingBufferDeclaration::new).merge(
            size != null ? size.asInt() : 0,
            waitStrategy != null ? rawTypeName(waitStrategy.asType()) : WaitStrategy.class.getName(),
            source);
        return ringBufferName;
    }

    private static boolean isRequestContextRequired(MethodInfo method) {
        return method.parameters().stream()
            .filter(parameter -> parameter.hasAnnotation(ObservesDisruptor.class))
            .map(parameter -> parameter.annotation(ObservesDisruptor.class).member("requestContext"))
            .anyMatch(requestContext -> requestContext != null && requestContext.asBoolean());
    }

    private boolean isQualifier(AnnotationInfo annotation) {
        return annotation.declaration().hasAnnotation(Qualifier.class);
    }

    private static boolean isAnyDefaultOrRingBuffer(AnnotationInfo annotation) {
        String name = annotation.name();
        return Any.class.getName().equals(name) || Default.class.getName().equals(name)
            || RingBuffer.class.getName().equals(name);
    }

    private static boolean hasParameterWith(MethodInfo method, Class<? extends java.lang.annotation.Annotation> type) {
        return method.parameters().stream().anyMatch(parameter -> parameter.hasAnnotation(type));
    }

    private static String observedTypeName(Type type) {
        if (type.isPrimitive()) {
            return PRIMITIVE_WRAPPERS.get(type.asPrimitive().primitiveKind().name().toLowerCase(Locale.ROOT));
        }
        return type.isClass() || type.isParameterizedType() ? rawTypeName(type) : null;
    }

    private static String rawTypeName(Type type) {
        if (type.isClass()) {
            return type.asClass().declaration().name();
        }
        if (type.isParameterizedType()) {
            return type.asParameterizedType().genericClass().declaration().name();
        }
        return null;
    }

    private static boolean isConcrete(Type type) {
        if (type.isClass() || type.isPrimitive()) {
            return true;
        }
        if (type.isParameterizedType()) {
            return type.asParameterizedType().typeArguments().stream()
                .allMatch(DisruptorBuildCompatibleExtension::isConcrete);
        }
        if (type.isArray()) {
            return isConcrete(type.asArray().componentType());
        }
        // type-variables and wildcards
        return false;
    }

    private static String canonical(Type type) {
        if (type.isParameterizedType()) {
            return rawTypeName(type) + type.asParameterizedType().typeArguments().stream()
                .map(DisruptorBuildCompatibleExtension::canonical)
                .collect(Collectors.joining(",", "<", ">"));
        }
        if (type.isArray()) {
            return canonical(type.asArray().componentType()) + "[]";
        }
        if (type.isPrimitive()) {
            return type.asPrimitive().primitiveKind().name();
        }
        String rawTypeName = rawTypeName(type);
        return rawTypeName != null ? rawTypeName : type.toString();
    }

    private static String canonical(AnnotationInfo annotation) {
        Map<String, String> members = new TreeMap<>();
        annotation.members().forEach((name, value) -> members.put(name, canonical(value)));
        return annotation.name() + members;
    }

    private static String canonical(AnnotationMember member) {
        switch (member.kind()) {
            case STRING:
                return "\"" + member.asString() + "\"";
            case ENUM:
                return member.asEnumClass().name() + "." + member.asEnumConstant();
            case CLASS:
                return canonical(member.asType());
            case NESTED_ANNOTATION:
                return canonical(member.asNestedAnnotation());
            case ARRAY:
                return member.asArray().stream()
                    .map(DisruptorBuildCompatibleExtension::canonical)
                    .collect(Collectors.joining(",", "[", "]"));
            case BOOLEAN:
                return String.valueOf(member.asBoolean());
            case CHAR:
                return String.valueOf(member.asChar());
            case FLOAT:
                return String.valueOf(member.asFloat());
            case DOUBLE:
                return String.valueOf(member.asDouble());
            default:
                return String.valueOf(member.asLong());
        }
    }

    private static String describe(FieldInfo field) {
        return field.declaringClass().name() + "#" + field.name();
    }

    private static String describe(MethodInfo method) {
        return method.declaringClass().name() + "#" + method.name();
    }

    private static String signature(MethodInfo method) {
        return method.name() + method.parameters().stream()
            .map(parameter -> canonical(parameter.type()))
            .collect(Collectors.joining(",", "(", ")"));
    }

    private static boolean[] toBooleanArray(List<Boolean> values) {
        boolean[] result = new boolean[values.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = values.get(i);
        }
        return result;
    }

    private static <T> Collection<T> concat(Collection<T> first, Collection<T> second) {
        return Stream.concat(first.stream(), second.stream()).collect(Collectors.toList());
    }

    private record Channel(int id,
                           String ringBufferName,
                           List<AnnotationInfo> qualifiers,
                           boolean circuitBreaker,
                           Map<String, Type> types) {
    }

    private record ObserverDefinition(InvokerInfo invoker,
                                      String observedType,
                                      List<AnnotationInfo> qualifiers,
                                      String ringBufferName,
                                      boolean circuitBreaker,
                                      int eventParameterPosition,
                                      int parameterCount,
                                      String description) {
    }
}

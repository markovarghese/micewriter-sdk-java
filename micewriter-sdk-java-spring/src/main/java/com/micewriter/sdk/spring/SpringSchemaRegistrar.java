package com.micewriter.sdk.spring;

import com.micewriter.sdk.annotation.IcebergEntity;
import com.micewriter.sdk.schema.SchemaRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Spring Boot lifecycle hook that discovers {@link IcebergEntity}-annotated classes via
 * classpath scanning and delegates registration to the core {@link SchemaRegistrar}.
 *
 * <p>Registered as a Spring bean by {@link com.micewriter.sdk.config.IcebergAutoConfiguration}.
 * Application code never needs to interact with this class directly.
 */
public class SpringSchemaRegistrar implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger log = LoggerFactory.getLogger(SpringSchemaRegistrar.class);

    private final SchemaRegistrar registrar;
    private final String basePackage;

    public SpringSchemaRegistrar(SchemaRegistrar registrar, String basePackage) {
        this.registrar = registrar;
        this.basePackage = basePackage;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(IcebergEntity.class));

        Set<BeanDefinition> candidates = scanner.findCandidateComponents(basePackage);
        log.info("Found {} @IcebergEntity class(es) via classpath scan (base: '{}')",
                candidates.size(), basePackage.isEmpty() ? "<root>" : basePackage);

        List<Class<?>> entityClasses = new ArrayList<>();
        for (BeanDefinition bd : candidates) {
            try {
                entityClasses.add(Class.forName(bd.getBeanClassName()));
            } catch (ClassNotFoundException e) {
                log.error("Could not load @IcebergEntity class {}: {}", bd.getBeanClassName(), e.getMessage());
            }
        }

        registrar.register(entityClasses.toArray(new Class<?>[0]));
    }
}

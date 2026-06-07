package com.micewriter.sdk.config;

import com.micewriter.sdk.ipc.UdsConnection;
import com.micewriter.sdk.schema.SchemaRegistrar;
import com.micewriter.sdk.spring.SpringSchemaRegistrar;
import com.micewriter.sdk.template.IcebergStreamTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot autoconfiguration for the mIceWriter SDK.
 *
 * Activated automatically via the {@code AutoConfiguration.imports} file.
 * Can be disabled with {@code micewriter.enabled=false}.
 */
@AutoConfiguration
@ConditionalOnProperty(name = "micewriter.enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(IcebergProperties.class)
public class IcebergAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public UdsConnection udsConnection(IcebergProperties props) {
        return new UdsConnection(
                props.getSocketPath(),
                props.getConnectTimeoutMs(),
                props.getAckTimeoutMs(),
                props.getMaxInFlightBytes()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public IcebergStreamTemplate icebergStreamTemplate(UdsConnection connection) {
        return new IcebergStreamTemplate(connection);
    }

    @Bean
    @ConditionalOnMissingBean
    public SchemaRegistrar schemaRegistrar(UdsConnection connection) {
        return new SchemaRegistrar(connection);
    }

    /**
     * Not {@code @ConditionalOnMissingBean} — always register so classpath
     * scanning fires on every startup.
     */
    @Bean
    public SpringSchemaRegistrar springSchemaRegistrar(SchemaRegistrar registrar,
                                                       IcebergProperties props) {
        return new SpringSchemaRegistrar(registrar, props.getBasePackage());
    }
}

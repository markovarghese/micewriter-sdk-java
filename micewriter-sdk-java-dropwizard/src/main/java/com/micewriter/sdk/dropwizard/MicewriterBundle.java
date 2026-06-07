package com.micewriter.sdk.dropwizard;

import com.micewriter.sdk.ipc.UdsConnection;
import com.micewriter.sdk.schema.SchemaRegistrar;
import com.micewriter.sdk.template.IcebergStreamTemplate;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.ConfiguredBundle;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Dropwizard bundle for the mIceWriter SDK.
 *
 * <p>Registers a managed lifecycle for the UDS connection and schema registrar,
 * mirroring the zero-boilerplate experience of the Spring Boot starter.
 *
 * <p>Usage:
 * <pre>{@code
 * public class App extends Application<AppConfig> {
 *
 *     private final MicewriterBundle<AppConfig> micewriter =
 *         new MicewriterBundle<>(AppConfig::getMicewriter)
 *             .entities(TelemetryEvent.class);
 *
 *     @Override
 *     public void initialize(Bootstrap<AppConfig> bootstrap) {
 *         bootstrap.addBundle(micewriter);
 *     }
 *
 *     @Override
 *     public void run(AppConfig config, Environment env) {
 *         env.jersey().register(new EventResource(micewriter.getTemplate()));
 *     }
 * }
 * }</pre>
 *
 * @param <T> the application's Dropwizard {@link Configuration} subclass
 */
public class MicewriterBundle<T extends Configuration> implements ConfiguredBundle<T> {

    private static final Logger log = LoggerFactory.getLogger(MicewriterBundle.class);

    private final Function<T, MicewriterConfig> configExtractor;
    private Class<?>[] entityClasses = new Class<?>[0];

    private volatile IcebergStreamTemplate template;

    /**
     * @param configExtractor lambda that extracts {@link MicewriterConfig} from the app config,
     *                        e.g. {@code AppConfig::getMicewriter}
     */
    public MicewriterBundle(Function<T, MicewriterConfig> configExtractor) {
        this.configExtractor = configExtractor;
    }

    /**
     * Declare the {@link com.micewriter.sdk.annotation.IcebergEntity}-annotated classes
     * whose schemas should be registered on startup.
     *
     * @return {@code this} for fluent chaining
     */
    public MicewriterBundle<T> entities(Class<?>... entityClasses) {
        this.entityClasses = entityClasses;
        return this;
    }

    @Override
    public void initialize(Bootstrap<?> bootstrap) {
        // Nothing to initialise before run().
    }

    @Override
    public void run(T appConfig, Environment environment) {
        MicewriterConfig cfg = configExtractor.apply(appConfig);

        UdsConnection connection = new UdsConnection(
                cfg.getSocketPath(),
                cfg.getConnectTimeoutMs(),
                cfg.getAckTimeoutMs(),
                cfg.getMaxInFlightBytes()
        );
        IcebergStreamTemplate localTemplate = new IcebergStreamTemplate(connection);
        SchemaRegistrar registrar = new SchemaRegistrar(connection);

        this.template = localTemplate;

        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() {
                log.info("mIceWriter: registering {} entity class(es)", entityClasses.length);
                registrar.register(entityClasses);
            }

            @Override
            public void stop() {
                log.info("mIceWriter: closing UDS connection");
                localTemplate.close();
                connection.close();
            }
        });
    }

    /**
     * Returns the {@link IcebergStreamTemplate} after the bundle has been run.
     * Call this from your application's {@code run()} method.
     *
     * @throws IllegalStateException if called before {@link #run} completes
     */
    public IcebergStreamTemplate getTemplate() {
        if (template == null) {
            throw new IllegalStateException(
                    "getTemplate() called before MicewriterBundle.run() — " +
                    "call it from Application.run(), not initialize()");
        }
        return template;
    }
}

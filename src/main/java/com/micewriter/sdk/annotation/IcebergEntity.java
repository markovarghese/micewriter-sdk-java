package com.micewriter.sdk.annotation;

import java.lang.annotation.*;

/**
 * Marks a POJO as an Iceberg-backed telemetry entity.
 *
 * The SDK will:
 *   1. Scan for this annotation on startup and register an Iceberg table schema.
 *   2. Allow instances to be streamed via {@code IcebergStreamTemplate.send()}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface IcebergEntity {

    /**
     * Iceberg table name. Defaults to the lower-cased simple class name when empty.
     */
    String table() default "";

    /**
     * Iceberg namespace path components. Matches {@code RegisterSchema.namespace} in the engine.
     */
    String[] namespace() default {"micewriter"};
}

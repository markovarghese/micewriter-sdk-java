package com.micewriter.sdk.annotation;

import java.lang.annotation.*;

/**
 * Marks a field within an {@link IcebergEntity} as the logical record identifier.
 * Informational only in the current implementation — the engine does not deduplicate
 * on this field but it is surfaced in schema metadata for downstream query tools.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface IcebergId {}

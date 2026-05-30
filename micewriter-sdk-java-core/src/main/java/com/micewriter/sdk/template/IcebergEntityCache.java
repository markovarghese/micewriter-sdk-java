package com.micewriter.sdk.template;

import com.micewriter.sdk.annotation.IcebergEntity;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

public final class IcebergEntityCache {
    private static final ConcurrentHashMap<Class<?>, byte[]> TABLE_NAME_CACHE = new ConcurrentHashMap<>();

    private IcebergEntityCache() {}

    public static byte[] getTableNameBytes(Class<?> clazz) {
        return TABLE_NAME_CACHE.computeIfAbsent(clazz, c -> {
            IcebergEntity ann = c.getAnnotation(IcebergEntity.class);
            if (ann == null) {
                throw new IllegalArgumentException(c.getName() + " must be annotated with @IcebergEntity");
            }
            String tableName = ann.table().isEmpty()
                    ? c.getSimpleName().toLowerCase(Locale.ROOT)
                    : ann.table();
            return tableName.getBytes(StandardCharsets.UTF_8);
        });
    }
}

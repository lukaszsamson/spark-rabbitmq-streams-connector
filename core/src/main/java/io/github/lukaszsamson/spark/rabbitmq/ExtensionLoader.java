package io.github.lukaszsamson.spark.rabbitmq;

/**
 * Utility for loading and instantiating connector extension classes.
 *
 * <p>Extension classes must:
 * <ul>
 *   <li>Implement the expected connector-defined interface</li>
 *   <li>Have a public no-arg constructor</li>
 * </ul>
 */
public final class ExtensionLoader {

    private ExtensionLoader() {}

    /**
     * Load, validate, and instantiate an extension class.
     *
     * @param className the fully-qualified class name
     * @param expectedType the interface the class must implement
     * @param optionName the option name (for error messages)
     * @param <T> the expected type
     * @return a new instance of the class
     * @throws IllegalArgumentException if the class cannot be loaded, does not implement
     *         the expected interface, or cannot be instantiated
     */
    @SuppressWarnings("unchecked")
    public static <T> T load(String className, Class<T> expectedType, String optionName) {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader fallbackLoader = ExtensionLoader.class.getClassLoader();
        Class<?> clazz;
        try {
            clazz = Class.forName(className, true,
                    contextLoader != null ? contextLoader : fallbackLoader);
        } catch (ClassNotFoundException e) {
            if (contextLoader != null && fallbackLoader != null && fallbackLoader != contextLoader) {
                try {
                    clazz = Class.forName(className, true, fallbackLoader);
                } catch (ClassNotFoundException ignored) {
                    throw new IllegalArgumentException(
                            "Class specified by '" + optionName + "' not found: " + className, e);
                }
            } else {
                throw new IllegalArgumentException(
                        "Class specified by '" + optionName + "' not found: " + className, e);
            }
        }

        if (!expectedType.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(
                    "Class specified by '" + optionName + "' (" + className +
                            ") does not implement " + expectedType.getName());
        }

        try {
            return (T) clazz.getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "Class specified by '" + optionName + "' (" + className +
                            ") must have a public no-arg constructor", e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate class specified by '" + optionName +
                            "': " + className, e);
        }
    }
}

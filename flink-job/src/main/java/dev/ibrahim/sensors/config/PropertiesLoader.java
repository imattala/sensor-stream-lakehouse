package dev.ibrahim.sensors.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertiesLoader {

    private static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{([^}]+)}");

    public static Properties load(String path) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
        }
        expandEnvVars(props);
        return props;
    }

    private static void expandEnvVars(Properties props) {
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            Matcher m = ENV_PATTERN.matcher(value);
            if (!m.find()) continue;

            StringBuffer sb = new StringBuffer();
            m.reset();
            while (m.find()) {
                String envKey = m.group(1);
                String envVal = System.getenv(envKey);
                if (envVal == null) {
                    throw new IllegalStateException(
                            "Required environment variable '" + envKey + "' is not set " +
                            "(referenced in config key '" + key + "')");
                }
                m.appendReplacement(sb, Matcher.quoteReplacement(envVal));
            }
            m.appendTail(sb);
            props.setProperty(key, sb.toString());
        }
    }
}

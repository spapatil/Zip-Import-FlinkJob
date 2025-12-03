package com.spandana.spandana_flink;

import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.Map;

public class ConfigLoader {
    public static Map<String, Object> loadConfig(String filePath) throws Exception {
        Yaml yaml = new Yaml();
        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(filePath)) {
            if (in == null) throw new RuntimeException("Config file not found: " + filePath);
            return yaml.load(in);
        }
    }
}

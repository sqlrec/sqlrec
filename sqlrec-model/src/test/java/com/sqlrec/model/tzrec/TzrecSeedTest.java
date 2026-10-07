package com.sqlrec.model.tzrec;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class TzrecSeedTest {
    @Test
    void seedControlsTorchAndNumpyInTrainingJob() {
        String yaml = TzrecK8sYamlUtils.createJobYaml("quality", "config", "headless", 1, 1, 29500,
                Map.of("random_seed", "456"));
        assertTrue(yaml.contains("TORCH_MANUAL_SEED"));
        assertTrue(yaml.contains("NUMPY_MANUAL_SEED"));
        assertTrue(yaml.contains("456"));
        assertFalse(TzrecK8sYamlUtils.createJobYaml("quality", "config", "headless", 1, 1, 29500, Map.of()).contains("MANUAL_SEED"));
        assertThrows(IllegalArgumentException.class, () -> TzrecK8sYamlUtils.createJobYaml("quality", "config", "headless", 1, 1, 29500,
                Map.of("random_seed", "-1")));
    }
}

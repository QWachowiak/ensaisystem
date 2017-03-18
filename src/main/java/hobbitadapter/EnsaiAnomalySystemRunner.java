package hobbitadapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Point d'entrée pour Docker.
 *
 */
public class EnsaiAnomalySystemRunner {
    private static final Logger logger = LoggerFactory.getLogger(EnsaiAnomalySystemRunner.class);

    public static void main(String... args) throws Exception {
        logger.debug("Running...");
        HobbitAdapter system = null;
        try {
            system = new HobbitAdapter();
            system.init();
            system.run();
        } finally {
            if (system != null) {
                system.close();
            }
        }
        logger.debug("Finished.");
    }
}
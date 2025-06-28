package source;
import java.time.*;
import java.io.IOException;
import java.util.logging.*;

public class GlobalLogger {
    private static Logger logger;
     private static FileHandler fh;
    static {
        try {

            fh = new FileHandler("log/" + LocalDate.now() + ".txt");
            logger = Logger.getLogger("GlobalLogger");
           
            fh.setFormatter(new SimpleFormatter());
            logger.addHandler(fh);
            logger.setUseParentHandlers(false);
            logger.setLevel(Level.ALL);
            
            fh.setLevel(Level.ALL);
           
           
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Logger getLogger() {
        return logger;
    }

    public static void closeLogger () {
        fh.close();
    }
}

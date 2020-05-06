package com.jirvan.jidbc;

import com.jirvan.util.DatabaseType;
import com.jirvan.util.Io;

import static com.jirvan.util.Assertions.*;

public abstract class DbUtils {

    private DatabaseType databaseType;

    protected DbUtils() {
    }

    protected DbUtils(DatabaseType databaseType) {
        this.databaseType = databaseType;
    }

    protected void executeDbScript(JidbcConnection jidbc, String scriptRelativePath) {
        assertNotNull(databaseType, "databaseType has not been set");
        executeScript(jidbc, databaseType.name() + "/" + scriptRelativePath);
    }

    protected void executeScript(JidbcConnection jidbc, String scriptRelativePath) {
        executeScript(jidbc, this.getClass(), scriptRelativePath);
    }

    public static void executeScript(JidbcConnection jidbc, Class<? extends DbUtils> anchorClass, String scriptRelativePath) {
        String script = Io.getResourceFileString(anchorClass, scriptRelativePath);
        for (String sql : script.replaceAll("(?m)^\\s+--.*$", "")
                                .replaceAll("^\\s*\\n+", "")
                                .replaceAll("(?m);\\s*\\n\\s*", ";\n")
                                .split("(?m); *\\n")) {
            jidbc.executeUpdate(sql);
        }
    }

}

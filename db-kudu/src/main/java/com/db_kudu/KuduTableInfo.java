package com.db_kudu;

import org.apache.commons.lang3.Validate;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;

import java.io.Serializable;

public class KuduTableInfo implements Serializable {

    private String name;
    private CreateTableOptionsFactory createTableOptionsFactory = null;
    private ColumnSchemasFactory schemasFactory = null;

    private KuduTableInfo(String name) {
        this.name = Validate.notNull(name);
    }

    public static KuduTableInfo forTable(String name) {
        return new KuduTableInfo(name);
    }

    public KuduTableInfo createTableIfNotExists(ColumnSchemasFactory schemasFactory, CreateTableOptionsFactory createTableOptionsFactory) {
        this.createTableOptionsFactory = Validate.notNull(createTableOptionsFactory);
        this.schemasFactory = Validate.notNull(schemasFactory);
        return this;
    }

    public Schema getSchema() {
        if (!getCreateTableIfNotExists()) {
            throw new RuntimeException("Cannot access schema for KuduTableInfo. Use createTableIfNotExists to specify the columns.");
        }
        return new Schema(schemasFactory.getColumnSchemas());
    }

    /**
     * @return Name of the table.
     */
    public String getName() {
        return name;
    }


    /**
     * @return True if table creation is enabled if target table does not exist.
     */
    public boolean getCreateTableIfNotExists() {
        return createTableOptionsFactory != null;
    }
    /**
     * @return CreateTableOptions if {@link #createTableIfNotExists} was specified.
     */
    public CreateTableOptions getCreateTableOptions() {
        if (!getCreateTableIfNotExists()) {
            throw new RuntimeException("Cannot access CreateTableOptions for KuduTableInfo. Use createTableIfNotExists to specify.");
        }
        return createTableOptionsFactory.getCreateTableOptions();
    }
}

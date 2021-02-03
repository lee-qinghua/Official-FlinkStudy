package com.db_kudu;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.kudu.client.SessionConfiguration;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public class KuduWriterConfig implements Serializable {
    private final String masters;
    private final SessionConfiguration.FlushMode flushMode;

    private KuduWriterConfig(
            String masters,
            SessionConfiguration.FlushMode flushMode) {

        this.masters = checkNotNull(masters, "Kudu masters cannot be null");
        this.flushMode = checkNotNull(flushMode, "Kudu flush mode cannot be null");
    }

    public String getMasters() {
        return masters;
    }

    public SessionConfiguration.FlushMode getFlushMode() {
        return flushMode;
    }

    public static class Builder {
        private String masters;
        private SessionConfiguration.FlushMode flushMode;

        private Builder(String masters) {
            this.masters = masters;
        }

        public static Builder setMasters(String masters) {
            return new Builder(masters);
        }

        public Builder setConsistency(SessionConfiguration.FlushMode flushMode) {
            this.flushMode = flushMode;
            return this;
        }

        public Builder setEventualConsistency() {
            return setConsistency(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        }

        public Builder setStrongConsistency() {
            return setConsistency(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        }

        public KuduWriterConfig build() {
            return new KuduWriterConfig(
                    masters,
                    flushMode);
        }
    }

}

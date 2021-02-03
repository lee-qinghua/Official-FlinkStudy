package com.db_kudu;

import org.apache.kudu.client.RowError;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface KuduFailureHandler extends Serializable {

    /**
     * Handle a failed {@link List < RowError >}.
     *
     * @param failure the cause of failure
     * @throws IOException if the sink should fail on this failure, the implementation should rethrow the throwable or a custom one
     */
    void onFailure(List<RowError> failure) throws IOException;

    /**
     * Handle a ClassCastException. Default implementation rethrows the exception.
     *
     * @param e the cause of failure
     * @throws IOException if the casting failed
     */
    default void onTypeMismatch(ClassCastException e) throws IOException {
        throw new IOException("Class casting failed \n", e);
    }
}
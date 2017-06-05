package org.chronopolis.intake.duracloud.batch.support;

import retrofit2.Callback;

import java.io.IOException;

/**
 * Response which resulted in an exception
 *
 * Created by shake on 6/5/17.
 */
public class ExceptingWrapper<E> extends CallWrapper<E> {

    public ExceptingWrapper() {
        super(null);
    }

    @Override
    public retrofit2.Response<E> execute() throws IOException {
        throw new IOException("test ioexception");
    }

    @Override
    public void enqueue(Callback<E> callback) {
        callback.onFailure(this, new IOException("test ioexception"));
    }

}

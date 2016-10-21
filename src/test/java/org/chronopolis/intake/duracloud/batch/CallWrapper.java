package org.chronopolis.intake.duracloud.batch;

import okhttp3.Request;
import retrofit2.Call;
import retrofit2.Callback;

import java.io.IOException;

/**
 * Wrap a Call for use in testing
 *
 * Created by shake on 10/14/16.
 */
public class CallWrapper<E> implements Call<E> {

    E e;

    public CallWrapper(E e) {
        this.e = e;
    }

    @Override
    public retrofit2.Response<E> execute() throws IOException {
        return retrofit2.Response.success(e);
    }

    @Override
    public void enqueue(Callback<E> callback) {
        callback.onResponse(this, retrofit2.Response.success(e));
    }

    @Override
    public boolean isExecuted() {
        return false;
    }

    @Override
    public void cancel() {
    }

    @Override
    public boolean isCanceled() {
        return false;
    }

    @Override
    public Call<E> clone() {
        return null;
    }

    @Override
    public Request request() {
        return new Request.Builder().build();
    }
}

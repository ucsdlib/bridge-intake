package org.chronopolis.intake.duracloud.batch.support;

import okhttp3.MediaType;
import okhttp3.ResponseBody;
import retrofit2.Callback;

import java.io.IOException;

/**
 * Response which represents an http 404
 *
 * Created by shake on 6/5/17.
 */
public class NotFoundWrapper<E> extends CallWrapper<E> {

    public NotFoundWrapper(E e) {
        super(e);
    }

    @Override
    public retrofit2.Response<E> execute() throws IOException {
        return retrofit2.Response.error(404, ResponseBody.create(MediaType.parse("application/json"), ""));
    }

    @Override
    public void enqueue(Callback<E> callback) {
        callback.onResponse(this, retrofit2.Response.<E>error(404, ResponseBody.create(MediaType.parse("application/json"), "")));
    }

}

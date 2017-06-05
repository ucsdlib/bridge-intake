package org.chronopolis.intake.duracloud.batch.support;

import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.ResponseBody;
import retrofit2.Callback;
import retrofit2.Response;

import java.io.IOException;

/**
 * Response representing an HTTP 400
 *
 * Created by shake on 6/5/17.
 */
public class BadRequestWrapper<E> extends CallWrapper<E> {

    public BadRequestWrapper(E e) {
        super(e);
    }

    @Override
    public retrofit2.Response<E> execute() throws IOException {
        Response<E> error = Response.<E>error(ResponseBody.create(MediaType.parse("text/plain"), "Test Bad Request"), new okhttp3.Response.Builder() //
                .code(400)
                .protocol(Protocol.HTTP_1_1)
                .request(new Request.Builder().url("http://localhost/").build())
                .build());
        return error;
    }

    @Override
    public void enqueue(Callback<E> callback) {
        callback.onResponse(this, Response.<E>error(400, ResponseBody.create(MediaType.parse("application/json"), "{message: 'Test Bad Request'}")));
    }

}

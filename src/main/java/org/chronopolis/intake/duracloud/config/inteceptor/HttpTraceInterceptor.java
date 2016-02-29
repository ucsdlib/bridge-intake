package org.chronopolis.intake.duracloud.config.inteceptor;

import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okio.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Interceptor to trace http calls
 *
 * Created by shake on 2/29/16.
 */
public class HttpTraceInterceptor implements Interceptor {
    private final Logger log = LoggerFactory.getLogger(HttpTraceInterceptor.class);

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        log.trace("{} {}", request.method(), request.url());
        if (request.body() != null) {
            Buffer sink = new Buffer();
            request.body().writeTo(sink);
            log.trace("{}", sink.readUtf8());
        }
        return chain.proceed(request);
    }

}

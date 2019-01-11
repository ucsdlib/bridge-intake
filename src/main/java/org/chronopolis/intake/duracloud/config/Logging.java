package org.chronopolis.intake.duracloud.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.util.FileSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

/**
 * Set up loggers to track operations on a per bridge level. How we configure these loggers locks
 * us in to an implementation, so in the event we move off of logback this will need to be updated.
 *
 * @author shake
 */
@SuppressWarnings("WeakerAccess")
public class Logging {

    private final Logger log = LoggerFactory.getLogger(Logging.class);

    public void createLogger(BridgeContext context) {
        log.info("[logging|{}] creating logger", context.getName());
        LoggerContext lc = (LoggerContext) StaticLoggerBinder.getSingleton().getLoggerFactory();

        // maybe break some of this out into configuration in the future
        String root = "/var/log/chronopolis/";
        String filename = root + context.getName() + ".log";

        RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setContext(lc);
        appender.setFile(filename);

        PatternLayoutEncoder ple = new PatternLayoutEncoder();
        ple.setContext(lc);
        ple.setPattern("%d{yyyy/MM/dd HH:mm:ss} %p %C{5} : %m%n");
        appender.setEncoder(ple);

        SizeBasedTriggeringPolicy<ILoggingEvent> trigger = new SizeBasedTriggeringPolicy<>();
        trigger.setContext(lc);
        trigger.setMaxFileSize(FileSize.valueOf("20MB"));
        appender.setTriggeringPolicy(trigger);

        FixedWindowRollingPolicy policy = new FixedWindowRollingPolicy();
        policy.setContext(lc);
        policy.setMaxIndex(5);
        policy.setParent(appender);
        policy.setFileNamePattern(filename + ".%i");
        appender.setRollingPolicy(policy);

        ple.start();
        policy.start();
        trigger.start();
        appender.start();

        ch.qos.logback.classic.Logger contextLogger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(context.getName());
        contextLogger.addAppender(appender);
        contextLogger.setLevel(Level.DEBUG);
        contextLogger.setAdditive(true);

        contextLogger.info("{} logger starting", context.getName());
    }
}

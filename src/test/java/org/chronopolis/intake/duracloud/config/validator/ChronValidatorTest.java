package org.chronopolis.intake.duracloud.config.validator;

import org.assertj.core.api.Assertions;
import org.assertj.core.util.Files;
import org.chronopolis.intake.duracloud.config.props.Chron;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.File;

public class ChronValidatorTest {

    private final Logger log = LoggerFactory.getLogger(ChronValidatorTest.class);

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private final AnnotationConfigApplicationContext context =
            new AnnotationConfigApplicationContext();

    @After
    public void close() {
        context.close();
    }

    @Test
    public void bindValid() {
        context.register(TinyApplication.class);
        File tmp = Files.newTemporaryFolder();
        tmp.deleteOnExit();
        TestPropertyValues.of("chron.workDirectory:" + tmp.toString())
                .applyTo(context);
        context.refresh();

        Chron chron = context.getBean(Chron.class);
        Assertions.assertThat(chron.getWorkDirectory()).isEqualTo(tmp.toString());
    }

    @Test
    public void bindFailCreate() {
        File tmp = Files.newTemporaryFolder();
        tmp.deleteOnExit();
        boolean result = tmp.setWritable(false);
        Assertions.assertThat(result).isTrue();

        context.register(TinyApplication.class);
        TestPropertyValues.of("chron.workDirectory:" + tmp.toString() + "/extra-dir")
                .applyTo(context);
        thrown.expect(BeanCreationException.class);
        context.refresh();
    }

    @Test
    public void bindFailDirectoryCheck() {
        File file = Files.newTemporaryFile();
        file.deleteOnExit();
        context.register(TinyApplication.class);
        TestPropertyValues.of("chron.workDirectory:" + file.toString())
                .applyTo(context);
        thrown.expect(BeanCreationException.class);
        context.refresh();
    }

    @Test
    @Ignore("does not work in gitlab-ci")
    public void bindFailRwe() {
        File tmp = Files.newTemporaryFolder();
        tmp.deleteOnExit();
        boolean result = tmp.setExecutable(false, false);
        Assertions.assertThat(result).isTrue();

        context.register(TinyApplication.class);
        TestPropertyValues.of("chron.workDirectory:" + tmp.toString())
                .applyTo(context);
        thrown.expect(BeanCreationException.class);
        context.refresh();
    }

    @Test
    public void bindNull() {
        context.register(TinyApplication.class);
        TestPropertyValues.of()
                .applyTo(context);
        thrown.expect(BeanCreationException.class);
        context.refresh();
    }

}
package org.chronopolis.intake.duracloud.notify;

import org.chronopolis.intake.duracloud.config.props.Smtp;
import org.junit.Test;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class MailNotifierTest {

    @Test
    public void testNotify() {
        final int reps = 10;
        final String title = "Mail Test";
        final String message = "test notify";
        JavaMailSender sender = mock(JavaMailSender.class);
        MailNotifier notifier = new MailNotifier(new Smtp(), sender);

        for (int i = 0; i < reps; i++) {
            notifier.notify(title, message);
        }

        verify(sender, times(1)).send(any(SimpleMailMessage.class));
    }

}
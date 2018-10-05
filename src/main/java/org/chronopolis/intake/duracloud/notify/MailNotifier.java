package org.chronopolis.intake.duracloud.notify;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.chronopolis.intake.duracloud.config.props.Smtp;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Simple SMTP notification based messages
 *
 * Created by shake on 3/1/17.
 */
public class MailNotifier implements Notifier {

    private final Smtp smtp;
    private final JavaMailSender sender;
    private final Set<String> seen = new ConcurrentSkipListSet<>();

    public MailNotifier(Smtp smtp) {
        this.smtp = smtp;
        this.sender = new JavaMailSenderImpl();
    }

    @VisibleForTesting
    MailNotifier(Smtp smtp, JavaMailSender sender) {
        this.smtp = smtp;
        this.sender = sender;
    }

    @Override
    public void notify(String title, String message) {
        // to prevent spamming duplicate messages, check against the title of the email (hashed)
        HashCode hash = Hashing.murmur3_128().hashString(title, Charset.defaultCharset());
        boolean send = seen.add(hash.toString());

        if (send) {
            SimpleMailMessage smm = new SimpleMailMessage();
            smm.setFrom(smtp.getFrom());
            smm.setTo(smtp.getTo());
            smm.setSubject(title);
            smm.setText(message);

            sender.send(smm);
        }
    }
}

package org.chronopolis.intake.duracloud.notify;

import org.chronopolis.intake.duracloud.config.props.Smtp;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

/**
 * Simple SMTP notification based messages
 *
 * Created by shake on 3/1/17.
 */
public class MailNotifier implements Notifier {

    private Smtp smtp;

    public MailNotifier(Smtp smtp) {
        this.smtp = smtp;
    }

    @Override
    public void notify(String title, String message) {
        SimpleMailMessage smm = new SimpleMailMessage();
        smm.setFrom(smtp.getFrom());
        smm.setTo(smtp.getTo());
        smm.setSubject(title);
        smm.setText(message);

        JavaMailSender sender = new JavaMailSenderImpl();
        sender.send(smm);
    }
}

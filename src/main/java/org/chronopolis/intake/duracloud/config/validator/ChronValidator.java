package org.chronopolis.intake.duracloud.config.validator;

import org.chronopolis.intake.duracloud.config.props.Chron;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.Errors;
import org.springframework.validation.ValidationUtils;
import org.springframework.validation.Validator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Simple validator for {@link Chron} ConfigurationProperties which checks against the
 * {@code workDirectory} property to validate its existence. This comes in a few steps:
 * 1) Check if it exists
 * 1a) If not, attempt to create
 * 2) Check that it's a directory
 * 3) Check that we can read, write, and execute
 *
 * @author shake
 */
public class ChronValidator implements Validator {

    private final Logger log = LoggerFactory.getLogger(ChronValidator.class);

    @Override
    public boolean supports(@NotNull Class<?> clazz) {
        return Chron.class == clazz;
    }

    @Override
    public void validate(Object o, @NotNull Errors errors) {
        ValidationUtils.rejectIfEmpty(errors, "workDirectory", "workDirectory.empty");

        Chron chron = (Chron) o;
        if (chron.getWorkDirectory() != null) {
            String path = chron.getWorkDirectory();
            Path temp = Paths.get(path);
            File asFile = temp.toFile();

            // 1
            if (!asFile.exists()) {
                Path parent = temp.getParent();
                try {
                    Files.createDirectories(parent);
                } catch (IOException e) {
                    log.error("Unable to create work directory {}!", path, e);
                    errors.reject("workDirectory", "Unable to create directory");
                    return; // I don't really like this but... whatever short circuit
                }
            }

            // 2 + 3
            if (!asFile.isDirectory()) {
                errors.reject("workDirectory", "Path is not a directory");
            } else if (!asFile.canExecute() || !asFile.canWrite() || !asFile.canRead()) {
                errors.reject("workDirectory",
                        "Permissions for path do now allow read/write/execute");
            }
        }
    }
}

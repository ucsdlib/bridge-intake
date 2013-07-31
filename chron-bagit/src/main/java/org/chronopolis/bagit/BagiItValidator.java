/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.chronopolis.bagit;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author shake
 */
public class BagiItValidator implements Validator {
    // As defined in the bagit spec
    private final String bagitRE = "bagit.txt";
    private final String versionRE = "BagIt-Version";
    private final String tagFileRE = "Tag-File-Character-Encoding";
    private final String tagFileEncoding = "UTF-8";
    private final Path bagitPath;

    private String bagVersion;

    public BagiItValidator(Path bag) {
        this.bagitPath = bag.resolve(bagitRE);
    }


    private boolean exists() {
        return bagitPath.toFile().exists();
    }

    @Override
    public boolean isValid() {
        boolean valid = exists();
        try {
            try (BufferedReader reader = Files.newBufferedReader(bagitPath, Charset.forName("UTF-8"))) {
                String line;
                while ( (line = reader.readLine()) != null ) {
                    String[] tmp = line.split(":");
                    if ( tmp.length > 2 ) {
                        valid = false;
                    }

                    // TODO: Maybe make it it's own method so I can just say
                    // valid = parseLine(tmp)
                    switch (tmp[1]) {
                        case versionRE:
                            bagVersion = tmp[2];
                            break;
                        case tagFileRE:
                            // Make sure we're using UTF-8
                            if ( !tmp[2].equals(tagFileEncoding)) {
                                valid = false;
                            }
                            break;
                        default:
                            valid = false;
                            break;
                    }
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(BagiItValidator.class.getName()).log(Level.SEVERE, null, ex);
        }
        return valid;
    }

    /**
     * @return the bagVersion
     */
    public String getBagVersion() {
        return bagVersion;
    }

    /**
     * @return the tagFileEncoding
     */
    public String getTagFileEncoding() {
        return tagFileEncoding;
    }


}

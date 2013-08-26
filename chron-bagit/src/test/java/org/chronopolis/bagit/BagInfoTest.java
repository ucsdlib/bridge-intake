/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.chronopolis.bagit;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.chronopolis.bagit.util.TagMetaElement;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.chronopolis.bagit.TestUtil.createReader;
import org.chronopolis.bagit.util.PayloadOxum;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 *
 * @author shake
 */
public class BagInfoTest {
    BagInfoProcessor bagInfoProcessor;
    private final String bagSizeRE = "Bag-Size";
    private final String baggingDateRE = "Bagging-Date";
    private final String oxumRE = "Payload-Oxum";
    private final DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-DD");
    private Path bagInfoPath;

    @Test
    public void testInit() throws IOException, URISyntaxException {
        URL bag = getClass().getResource("/individual/bag-info-valid.txt");
        Path bagPath = Paths.get(bag.toURI());
        bagInfoProcessor = new BagInfoProcessor(bagPath);
        bagInfoProcessor.setBagInfoPath(bagPath);


        Assert.assertEquals(1, bagInfoProcessor.getPayloadOxum().getNumFiles());
        Assert.assertEquals(18, 
                            bagInfoProcessor.getPayloadOxum().getOctetCount());
        Assert.assertEquals(bagSizeRE, bagInfoProcessor.getBagSize().getKey());
        Assert.assertEquals("18 K", bagInfoProcessor.getBagSize().getValue());
        Assert.assertEquals(baggingDateRE, 
                            bagInfoProcessor.getBaggingDate().getKey());
        //Assert.assertEquals(date, bagInfoProcessor.getBaggingDate().getValue());
    }

    

    @Test
    public void testValid() throws IOException, Exception {
        URL bag = getClass().getResource("/individual/bag-info-valid.txt");
        URL oxum = getClass().getResource("/bags/validbag-256/data");
        Path bagPath = Paths.get(bag.toURI());
        bagInfoProcessor = new BagInfoProcessor(bagPath);
        bagInfoProcessor.setBagInfoPath(bagPath);

        PayloadOxum actualOxum = new PayloadOxum();
        actualOxum.calculateOxum(Paths.get(oxum.toURI()));
        bagInfoProcessor.setPayloadOxum(actualOxum);
        /*
        */
        boolean valid = bagInfoProcessor.valid();
         
        Assert.assertTrue(valid);
    }

    @Test
    public void testInvalidFile() throws IOException, Exception {
        URL bag = getClass().getResource("/individual/bag-info-invalid.txt");
        URL oxum = getClass().getResource("/bags/validbag-256/data");
        Path bagPath = Paths.get(bag.toURI());
        bagInfoProcessor = new BagInfoProcessor(bagPath);

        PayloadOxum actualOxum = new PayloadOxum();
        actualOxum.calculateOxum(Paths.get(oxum.toURI()));
        bagInfoProcessor.setPayloadOxum(actualOxum);

        Assert.assertFalse(bagInfoProcessor.valid());
    }
    
}

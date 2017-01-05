package org.chronopolis.intake.duracloud.batch.support;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.writer.SimpleBagWriter;
import org.chronopolis.bag.writer.WriteResult;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * Created by shake on 1/4/17.
 */
public class TarBagWriter extends SimpleBagWriter {

    @Override
    public List<WriteResult> write(List<Bag> bags) {
        return bags.stream()
                .map(this::fromBag)
                .collect(Collectors.toList());
    }

    private WriteResult fromBag(Bag bag) {
        return null;
    }

}

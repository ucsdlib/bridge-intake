package org.chronopolis.ingest.repository;

import org.chronopolis.rest.models.ReplicationStatus;
import org.chronopolis.rest.models.Restoration;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;

/**
 * Created by shake on 12/8/14.
 */
public interface RestoreRepository extends JpaRepository<Restoration, Long> {

    Restoration findByNodeUsername(String username);
    Restoration findByNameAndDepositor(String name, String depositor);
    Collection<Restoration> findByStatus(ReplicationStatus status);

}

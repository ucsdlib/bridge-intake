package org.chronopolis.ingest.api;

import org.chronopolis.ingest.controller.ControllerUtil;
import org.chronopolis.ingest.exception.NotFoundException;
import org.chronopolis.ingest.repository.BagRepository;
import org.chronopolis.ingest.repository.BagService;
import org.chronopolis.ingest.repository.NodeRepository;
import org.chronopolis.ingest.repository.ReplicationSearchCriteria;
import org.chronopolis.ingest.repository.ReplicationService;
import org.chronopolis.rest.models.Bag;
import org.chronopolis.rest.models.BagStatus;
import org.chronopolis.rest.models.Node;
import org.chronopolis.rest.models.Replication;
import org.chronopolis.rest.models.ReplicationRequest;
import org.chronopolis.rest.models.ReplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.Principal;
import java.util.Map;
import java.util.Set;

import static org.chronopolis.ingest.api.Params.PAGE;
import static org.chronopolis.ingest.api.Params.PAGE_SIZE;
import static org.chronopolis.ingest.api.Params.STATUS;

/**
 * REST controller for replication methods
 * <p/>
 * TODO: We'll probably want a separate class to handle common db stuff
 * using the 3 repository classes
 * <p/>
 * Created by shake on 11/5/14.
 */
@RestController
@RequestMapping("/api/replications")
public class ReplicationController {
    private final Logger log = LoggerFactory.getLogger(ReplicationController.class);

    @Autowired
    NodeRepository nodeRepository;

    @Autowired
    BagService bagService;

    @Autowired
    ReplicationService replicationService;

    /**
     * Create a replication request for a given node and bag
     * <p/>
     * TODO: Return a 201
     *
     * @param principal - authentication information
     * @param request   - request containing the bag id to replicate
     * @return
     */
    @RequestMapping(method = RequestMethod.POST)
    public Replication createReplication(Principal principal,
                                         @RequestBody ReplicationRequest request) {
        // Create a new replication for the Node (user) based on the Bag ID
        // Return a 404 if the bag is not found
        // If a replication already exists, return it instead of creating a new one
        Node node = nodeRepository.findByUsername(principal.getName());
        Bag bag = bagService.findBag(request.getBagID());

        if (bag == null) {
            throw new NotFoundException("Bag " + request.getBagID());
        }

        ReplicationSearchCriteria criteria = new ReplicationSearchCriteria()
                .withBagId(bag.getID())
                .withNodeUsername(node.getUsername());

        // TODO: This can actually return multiple replications, we'll want to filter as well
        Replication action = replicationService.getReplication(criteria);

        if (action == null) {
            log.info("Creating new replication for node {} and bag {}",
                    node.getUsername(),
                    bag.getID());

            action = new Replication(node, bag);
            replicationService.save(action);
        }
        return action;
    }

    /**
     * Update a given replication based on the id of the path used
     * TODO: Update state properly
     *
     * @param principal     - authentication information
     * @param replicationID - the id of the replication to update
     * @param replication   - the updated replication sent from the client
     * @return
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.PUT)
    public Replication updateReplication(Principal principal,
                                         @PathVariable("id") Long replicationID,
                                         @RequestBody Replication replication) {
        ReplicationSearchCriteria criteria = new ReplicationSearchCriteria()
                .withId(replicationID);

        // If a user is not an admin, make sure we only search for THEIR replications
        if (!ControllerUtil.hasRoleAdmin()) {
            criteria.withNodeUsername(principal.getName());
        }
        Replication update = replicationService.getReplication(criteria);

        if (update == null) {
            throw new NotFoundException("Replication " + replicationID);
        }

        Node node = update.getNode();
        Bag bag = update.getBag();

        // TODO: Move logic outside of here? (yes)
        log.info("Updating replication {}", replication.getID());

        String receivedTokenFixity = replication.getReceivedTokenFixity();
        String receivedTagFixity = replication.getReceivedTagFixity();
        boolean success = true;

        // Only update status if we were given a fixity value
        if (receivedTokenFixity != null) {
            log.debug("Received token fixity of {}", receivedTokenFixity);
            update.setReceivedTokenFixity(receivedTokenFixity);

            String digest = bag.getTokenDigest();
            // Check against the stored digest
            if (digest == null || !digest.equals(receivedTokenFixity)) {
                log.info("Received invalid token store fixity for bag {} from {}",
                        bag.getID(),
                        node.getUsername());
                update.setStatus(ReplicationStatus.FAILURE_TOKEN_STORE);
                success = false;
            }
        } else {
            success = false;
        }

        if (receivedTagFixity != null) {
            log.debug("Received tag fixity of {}", receivedTagFixity);
            update.setReceivedTagFixity(receivedTagFixity);

            String digest = bag.getTagManifestDigest();
            // Check against the stored digest
            if (digest == null || !digest.equals(receivedTagFixity)) {
                log.info("Received invalid tagmanifest fixity for bag {} from {}",
                        bag.getID(),
                        node.getUsername());
                update.setStatus(ReplicationStatus.FAILURE_TAG_MANIFEST);
                success = false;
            }
        } else {
            success = false;
        }

        // If we were able to validate all the manifests: yay
        // else check if the replicating node reported any problems
        // TODO: Hold out on failure until x number of times?
        if (success) {
            update.setStatus(ReplicationStatus.SUCCESS);
            Set<Node> nodes = bag.getReplicatingNodes();
            nodes.add(node);

            // And last check to see if the bag has been replicated
            if (nodes.size() >= bag.getRequiredReplications()) {
                log.debug("Setting bag {}::{} as replicated",
                        bag.getDepositor(),
                        bag.getName());
                bag.setStatus(BagStatus.REPLICATED);
            }
        } else if (isClientStatus(replication.getStatus())   // Check if the client is giving us a status
                && !isFailureStatus(update.getStatus())) {   // and that we haven't already set a failed state
            log.info("Updating status from client");
            update.setStatus(replication.getStatus());
        }

        replicationService.save(update);
        return update;
    }

    /**
     * Return true if this is a status set by the client
     *
     * TODO: This can be done in the enumerated type.
     *
     * @param status
     * @return
     */
    private boolean isClientStatus(ReplicationStatus status) {
        return status == ReplicationStatus.STARTED
            || status == ReplicationStatus.TRANSFERRED
            || status == ReplicationStatus.FAILURE;
    }

    /**
     * Return true if the status is a failure mode set by the server
     *
     * @param status
     * @return
     */
    private boolean isFailureStatus(ReplicationStatus status) {
        return status == ReplicationStatus.FAILURE_TOKEN_STORE
            || status == ReplicationStatus.FAILURE_TAG_MANIFEST;
    }

    /**
     * Retrieve all replications associated with a particular node/user
     *
     * @param principal - authentication information
     * @param params    - query parameters used for searching
     * @return
     */
    @RequestMapping(method = RequestMethod.GET)
    public Iterable<Replication> replications(Principal principal,
                                              @RequestParam Map<String, String> params) {
        Integer page = params.containsKey(PAGE)
                ? Integer.parseInt(params.get(PAGE))
                : 0;
        Integer pageSize = params.containsKey(PAGE_SIZE)
                ? Integer.parseInt(params.get(PAGE_SIZE))
                : 20;
        String name = null;
        if (!ControllerUtil.hasRoleAdmin()) {
            name = principal.getName();
        }

        // null is handled fine so we can set that as a default
        ReplicationStatus status = params.containsKey(STATUS)
                ? ReplicationStatus.valueOf(params.get(STATUS))
                : null;

        // TODO: May want a function to build the criteria from the request params
        ReplicationSearchCriteria criteria = new ReplicationSearchCriteria()
                .withNodeUsername(name)
                .withStatus(status);

        return replicationService.getReplications(criteria, new PageRequest(page, pageSize));
    }

    /**
     * Retrieve a single replication based on its ID
     *
     * @param principal - authentication information
     * @param actionId  - the ID to search for
     * @return
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public Replication findReplication(Principal principal,
                                       @PathVariable("id") Long actionId) {
        Replication action = replicationService.getReplication(
                new ReplicationSearchCriteria().withId(actionId)
        );

        return action;
    }

}
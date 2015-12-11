package org.chronopolis.ingest.repository;

import org.chronopolis.ingest.models.UserRequest;
import org.chronopolis.rest.models.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.provisioning.UserDetailsManager;
import org.springframework.stereotype.Component;

import javax.transaction.Transactional;
import java.util.Collection;
import java.util.HashSet;

/**
 * Service to handle things related to user accounts
 *
 * Created by shake on 6/10/15.
 */
@Component
@Transactional
public class UserService {

    private final Logger log = LoggerFactory.getLogger(UserService.class);

    @Autowired
    UserDetailsManager manager;

    @Autowired
    NodeRepository repository;

    public void createUser(UserRequest request) {
        if (manager.userExists(request.getUsername())) {
            log.debug("User already exists, avoiding creation");
            return;
        }

        log.info("Creating new user {}", request.getUsername());
        String username = request.getUsername();
        String password = request.getPassword();

        Collection<SimpleGrantedAuthority> authorities = new HashSet<>();

        // Since we only have 2 roles at the moment it's easy to create users like this,
        // but we really should update this to have the authorities sent in the request
        if (request.isAdmin()) {
            authorities.add(new SimpleGrantedAuthority("ROLE_ADMIN"));
        } else {
            authorities.add(new SimpleGrantedAuthority("ROLE_USER"));
        }

        UserDetails userDetails = new User(username, password, authorities);
        manager.createUser(userDetails);

        // Add node if requested
        if (request.isNode()) {
            log.debug("Creating node for {}", username);
            if (repository.findByUsername(username) == null) {
                repository.save(new Node(username, password));
            }
        }
    }

    public UserDetails getUser(String username) {
        return manager.loadUserByUsername(username);
    }

}

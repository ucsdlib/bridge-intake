package org.chronopolis.ingest.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * Some utility functions for our controllers
 *
 * Created by shake on 4/20/15.
 */
public class ControllerUtil {
    private static final Logger log = LoggerFactory.getLogger(ControllerUtil.class);

    /**
     * Determine if the user in our current context has administrative privileges
     *
     * @return
     */
    public static boolean hasRoleAdmin() {
        UserDetails userDetails = (UserDetails) SecurityContextHolder.getContext()
                .getAuthentication()
                .getPrincipal();

        for (GrantedAuthority authority : userDetails.getAuthorities()) {
            if (authority.getAuthority().equalsIgnoreCase("ROLE_ADMIN")) {
                return true;
            }
        }

        log.trace("User {} does not have admin role", userDetails.getUsername());
        return false;
    }

}
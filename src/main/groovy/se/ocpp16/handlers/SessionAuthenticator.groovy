package se.ocpp16.handlers

import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import se.EdgeConfig
import se.service.AuthCacheService

import java.security.MessageDigest

@Singleton
class SessionAuthenticator {
    Logger logger = LoggerFactory.getLogger(SessionAuthenticator.class);

    @Inject
    EdgeConfig edgeConfig

    @Inject
    AuthCacheService authCacheService

    /**
     *
     * @param identifier
     * @param username
     * @param password
     * @return
     */
    public boolean authenticate(String identifier, String username, byte[] password) {
        boolean logPassword = edgeConfig.logPassword // Set to true only in a secure environment
        try {
            if (logPassword) {
                logger.info("Authorizing websocket connection for identifier-{} with username-{} and password-{}", identifier, username, password == null ? "<null>" : new String(password))
            } else {
                logger.info("Authorizing websocket connection for identifier-{} with username-{} and password-<hidden>", identifier, username)
            }
            if (username == null) {
                username = identifier
            }
            logger.info("Authorizing websocket connection for {}", identifier)
            def needsAuth = authCacheService.shouldAuthenticate(identifier)
            if (!needsAuth)
                return true

            if (username == null || password == null) {
                logger.error("Invalid username or password input for ${identifier}");
                return false;
            }

            // Convert password to string
            String passwordStr = new String(password);

            // Check for empty username or password
            if (username.isEmpty() || passwordStr.isEmpty()) {
                logger.error("Empty username or password input for $identifier");
                return false;
            }

            def credentials = authCacheService.getChargerCredentials().get(identifier)

            //logger.trace("Authentication username {} api pwd {} and charger pwd for {}", username, credentials?.password, hashPassword(passwordStr))

            if (credentials.chargerId.equalsIgnoreCase(username) && credentials.password == hashPassword(passwordStr)) {
                logger.info("Authentication successful for {}", identifier);
                return true
            } else {
                logger.error("Authentication failed: Invalid username or password for ${identifier}");
                return false
            }
        } catch (Exception e) {
            logger.error("Unsuccessful authentication of ${identifier}", e.getMessage(),e)
            return false
        }
    }

    String hashPassword(String password) {
        MessageDigest md = MessageDigest.getInstance("SHA-1")
        byte[] hashBytes = md.digest(password.bytes)
        return hashBytes.collect { String.format("%02x", it) }.join('')
    }
}

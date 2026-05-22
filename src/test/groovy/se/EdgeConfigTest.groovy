package se;

import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for verifying the configuration properties in EdgeConfig.
 * This class uses Micronaut's dependency injection and configuration
 * management features to inject and test configuration properties.
 */
@MicronautTest
/**
 * Sets the configuration property 'se.config.authBypass' to 'true' for this test.
 * <p>
 * This annotation is used to override the 'authBypass' property in the application
 * configuration specifically for this test class. By setting this property to 'true',
 * the test ensures that the 'authBypass' behavior is enabled during the test execution.
 */
@Property(name = "se.config.authBypass", value = "true")
@Property(name = "se.config.wss.enabled", value = "true")
class EdgeConfigTest {

  @Inject
  EdgeConfig edgeConfig;

  /**
   * Test method to verify the values of configuration properties in EdgeConfig.
   * This method asserts that the properties are correctly injected and have the
   * expected values.
   */
  @Test
  void testEdgeConfigValues() {
    // Verify the general configuration properties
    assertEquals("main", edgeConfig.getHandler(), "Handler should be 'main'");
    assertEquals("boundedElastic", edgeConfig.getScheduler(), "Scheduler should be 'boundedElastic'");
    assertTrue(edgeConfig.isAuthBypass(), "AuthBypass should be enabled");

    // Verify the WebSocket SSL (WSS) configuration properties
    EdgeConfig.WSSConfig wssConfig = edgeConfig.getWss();
    assertNotNull(wssConfig, "WSSConfig should not be null");
    assertTrue(wssConfig.isEnabled(), "WSS should be enabled");
    assertEquals("TLS", wssConfig.getEncryptionType(), "Encryption type should be 'TLS'");

    // Verify the Chargers configuration properties
    EdgeConfig.Chargers chargersConfig = edgeConfig.getChargers();
    assertNotNull(chargersConfig, "ChargersConfig should not be null");
    // Additional assertions can be added here for other properties in ChargersConfig
  }
}

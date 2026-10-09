package dev.relayops;
import org.junit.jupiter.api.Test;
import jakarta.validation.Validation;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
class DispatchValidationTest {
 private final jakarta.validation.Validator validator=Validation.buildDefaultValidatorFactory().getValidator();
 @Test void validDispatch() { assertTrue(validator.validate(new DispatchController.DispatchRequest("order-42","flaky",Map.of("order",42))).isEmpty()); }
 @Test void missingIdempotencyKeyRejected() { assertFalse(validator.validate(new DispatchController.DispatchRequest("","healthy",Map.of())).isEmpty()); }
 @Test void unregisteredDestinationRejected() { assertFalse(validator.validate(new DispatchController.DispatchRequest("x","http://internal",Map.of())).isEmpty()); }
 @Test void payloadRequired() { assertFalse(validator.validate(new DispatchController.DispatchRequest("x","healthy",null)).isEmpty()); }
}

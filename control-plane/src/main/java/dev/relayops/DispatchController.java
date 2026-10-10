package dev.relayops;
import java.util.Map;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.*;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/api")
public class DispatchController {
 private final RestClient client;
 public DispatchController(@Value("${relay.worker-url}") String url,@Value("${relay.worker-key:}") String key) {
   client=RestClient.builder().baseUrl(url).defaultHeader("X-API-Key",key).build();
 }
 public record DispatchRequest(@NotBlank @Size(max=200) String key,
   @Pattern(regexp="[a-zA-Z][a-zA-Z0-9_-]{0,39}") @NotNull String endpoint,@NotNull Map<String,Object> payload) {}
 @PostMapping("/events") public ResponseEntity<String> dispatch(@Valid @RequestBody DispatchRequest body) {
   return client.post().uri("/api/events").contentType(MediaType.APPLICATION_JSON).body(body).retrieve().toEntity(String.class);
 }
 @GetMapping("/healthz") public ResponseEntity<String> health() { return get("/healthz"); }
 @GetMapping("/destinations") public ResponseEntity<String> destinations() { return get("/api/destinations"); }
 @GetMapping("/metrics") public ResponseEntity<String> metrics() { return get("/api/metrics"); }
 @GetMapping("/events") public ResponseEntity<String> events(@RequestParam(required=false) String status,
    @RequestParam(defaultValue="100") int limit) {
   if(limit<1 || limit>500)return ResponseEntity.badRequest().body("{\"detail\":\"limit must be 1–500\"}");
   var builder=UriComponentsBuilder.fromPath("/api/events").queryParam("limit",limit);
   if(status!=null)builder.queryParam("status",status);
   return get(builder.build().encode().toUriString());
 }
 @GetMapping("/events/{id}/attempts") public ResponseEntity<String> attempts(@PathVariable String id) {
   return get("/api/events/"+java.util.UUID.fromString(id)+"/attempts");
 }
 @PostMapping("/events/{id}/replay") public ResponseEntity<String> replay(@PathVariable String id) {
   return client.post().uri("/api/events/"+java.util.UUID.fromString(id)+"/replay").retrieve().toEntity(String.class);
 }
 private ResponseEntity<String> get(String path) { return client.get().uri(path).retrieve().toEntity(String.class); }
 @ExceptionHandler(RestClientResponseException.class) public ResponseEntity<String> downstream(RestClientResponseException e) {
   return ResponseEntity.status(e.getStatusCode()).contentType(MediaType.APPLICATION_JSON).body(e.getResponseBodyAsString());
 }
 @ExceptionHandler(ResourceAccessException.class) public ResponseEntity<String> unavailable(ResourceAccessException e) {
   return ResponseEntity.status(503).contentType(MediaType.APPLICATION_JSON).body("{\"detail\":\"Delivery service unavailable\"}");
 }
 @ExceptionHandler(IllegalArgumentException.class) public ResponseEntity<String> badId(IllegalArgumentException e) {
   return ResponseEntity.badRequest().contentType(MediaType.APPLICATION_JSON).body("{\"detail\":\"Invalid event ID\"}");
 }
}

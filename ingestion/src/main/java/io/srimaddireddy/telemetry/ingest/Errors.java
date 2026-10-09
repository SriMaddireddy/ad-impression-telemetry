package io.srimaddireddy.telemetry.ingest;
import java.util.*;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
@RestControllerAdvice
public class Errors {
 @ExceptionHandler(IllegalArgumentException.class) public ResponseEntity<?> invalid(IllegalArgumentException e){return ResponseEntity.badRequest().body(Map.of("error",e.getMessage()));}
 @ExceptionHandler(NoSuchElementException.class) public ResponseEntity<?> missing(){return ResponseEntity.status(404).body(Map.of("error","Unknown campaign"));}
 @ExceptionHandler(IllegalStateException.class) public ResponseEntity<?> unavailable(){return ResponseEntity.status(503).body(Map.of("error","Pipeline unavailable; some Kafka records may already be acknowledged. Retries can duplicate events."));}
}

package com.github.tennyros.mockservice;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/response")
public class StatusCheckController {

    @GetMapping("/200")
    public ResponseEntity<String> get200() {
        return ResponseEntity.ok("200");
    }

    @GetMapping("/500")
    public ResponseEntity<String> get500() {
        return ResponseEntity.internalServerError().build();
    }
}

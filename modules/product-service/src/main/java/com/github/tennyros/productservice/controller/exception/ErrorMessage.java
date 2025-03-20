package com.github.tennyros.productmicroservice.controller.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@AllArgsConstructor
public class ErrorMessage {

    private LocalDateTime timestamp;
    private String message;

}

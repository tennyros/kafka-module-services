package com.github.tennyros.productmicroservice.service;

import com.github.tennyros.productmicroservice.service.dto.CreateProductDto;

import java.util.concurrent.ExecutionException;

public interface ProductService {

    String createProduct(CreateProductDto createProductDto) throws ExecutionException, InterruptedException;
}

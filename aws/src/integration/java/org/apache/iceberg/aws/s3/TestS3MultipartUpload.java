/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;
import software.amazon.awssdk.services.s3.S3Client;

/** Long-running tests to ensure multipart upload logic is resilient */
@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_ACCESS_KEY_ID, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SECRET_ACCESS_KEY, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SESSION_TOKEN, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_REGION, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_TEST_BUCKET, matches = ".*")
})
public class TestS3MultipartUpload {

  private final Random random = new Random(1);
  private static S3Client s3;
  private static String bucketName;
  private static String prefix;
  private static S3FileIOProperties properties;
  private static S3FileIO io;
  private String objectUri;

  @BeforeAll
  public static void beforeClass() {
    s3 = AwsClientFactories.defaultFactory().s3();
    bucketName = AwsIntegTestUtil.testBucketName();
    prefix = UUID.randomUUID().toString();
    properties = new S3FileIOProperties();
    properties.setMultiPartSize(S3FileIOProperties.MULTIPART_SIZE_MIN);
    properties.setChecksumEnabled(true);
    io = new S3FileIO(() -> s3);
    io.initialize(
        ImmutableMap.of(
            S3FileIOProperties.MULTIPART_SIZE,
            Integer.toString(S3FileIOProperties.MULTIPART_SIZE_MIN),
            S3FileIOProperties.CHECKSUM_ENABLED,
            "true"));
  }

  @AfterAll
  public static void afterClass() {
    AwsIntegTestUtil.cleanS3GeneralPurposeBucket(s3, bucketName, prefix);
  }

  @BeforeEach
  public void before() {
    String objectKey = String.format("%s/%s", prefix, UUID.randomUUID());
    objectUri = String.format("s3://%s/%s", bucketName, objectKey);
  }

  @Test
  public void testManyPartsWriteWithInt() {
    int parts = 200;
    writeInts(objectUri, parts, random::nextInt);
    assertThat(io.newInputFile(objectUri).getLength())
        .isEqualTo(parts * (long) S3FileIOProperties.MULTIPART_SIZE_MIN);
  }

  @Test
  public void testManyPartsWriteWithBytes() {
    int parts = 200;
    byte[] bytes = new byte[S3FileIOProperties.MULTIPART_SIZE_MIN];
    writeBytes(
        objectUri,
        parts,
        () -> {
          random.nextBytes(bytes);
          return bytes;
        });
    assertThat(io.newInputFile(objectUri).getLength())
        .isEqualTo(parts * (long) S3FileIOProperties.MULTIPART_SIZE_MIN);
  }

  @Test
  public void testContentsWriteWithInt() throws IOException {
    writeInts(objectUri, 10, () -> 6);
    verifyInts(objectUri, () -> 6);
  }

  @Test
  public void testContentsWriteWithBytes() throws IOException {
    byte[] bytes = new byte[S3FileIOProperties.MULTIPART_SIZE_MIN];
    for (int i = 0; i < S3FileIOProperties.MULTIPART_SIZE_MIN; i++) {
      bytes[i] = 6;
    }
    writeBytes(objectUri, 10, () -> bytes);
    verifyInts(objectUri, () -> 6);
  }

  @Test
  public void testUploadRemainder() throws IOException {
    long length = 3 * S3FileIOProperties.MULTIPART_SIZE_MIN + 2 * 1024 * 1024;
    writeInts(objectUri, 1, length, random::nextInt);
    assertThat(io.newInputFile(objectUri).getLength()).isEqualTo(length);
  }

  @Test
  public void testParallelUpload() throws IOException {
    int threads = 16;
    IntStream.range(0, threads).parallel().forEach(d -> writeInts(objectUri + d, 3, () -> d));

    for (int i = 0; i < threads; i++) {
      final int d = i;
      verifyInts(objectUri + d, () -> d);
    }
  }

  private void writeInts(String fileUri, int parts, Supplier<Integer> writer) {
    writeInts(fileUri, parts, S3FileIOProperties.MULTIPART_SIZE_MIN, writer);
  }

  private void writeInts(String fileUri, int parts, long partSize, Supplier<Integer> writer) {
    try (PositionOutputStream outputStream = io.newOutputFile(fileUri).create()) {
      for (int i = 0; i < parts; i++) {
        for (long j = 0; j < partSize; j++) {
          outputStream.write(writer.get());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void verifyInts(String fileUri, Supplier<Integer> verifier) {
    try (SeekableInputStream inputStream = io.newInputFile(fileUri).newStream()) {
      int cur;
      while ((cur = inputStream.read()) != -1) {
        assertThat(cur).isEqualTo(verifier.get());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeBytes(String fileUri, int parts, Supplier<byte[]> writer) {
    try (PositionOutputStream outputStream = io.newOutputFile(fileUri).create()) {
      for (int i = 0; i < parts; i++) {
        outputStream.write(writer.get());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

package net.pincette.s3.util;

import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;
import static org.reactivestreams.FlowAdapters.toPublisher;
import static software.amazon.awssdk.core.async.AsyncRequestBody.empty;
import static software.amazon.awssdk.core.async.AsyncRequestBody.fromPublisher;
import static software.amazon.awssdk.core.async.SdkPublisher.fromIterable;
import static software.amazon.awssdk.services.s3.S3AsyncClient.create;
import static software.amazon.awssdk.services.s3.model.MetadataDirective.REPLACE;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import net.pincette.util.Pair;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/**
 * @author Werner Donné
 */
public class Util {
  private static final S3AsyncClient client = create();

  private Util() {}

  private static String addSlash(final String s) {
    return s.endsWith("/") ? s : (s + "/");
  }

  private static CopyObjectRequest copyObjectRequest(
      final String bucket, final String key, final Map<String, String> metadata) {
    return CopyObjectRequest.builder()
        .sourceBucket(bucket)
        .destinationBucket(bucket)
        .sourceKey(key)
        .destinationKey(key)
        .metadata(metadata)
        .metadataDirective(REPLACE)
        .build();
  }

  public static CompletionStage<PutObjectResponse> createFolder(
      final String bucket, final String key) {
    return createFolder(createFolderObjectRequest(bucket, addSlash(key)));
  }

  private static CompletionStage<PutObjectResponse> createFolder(final PutObjectRequest request) {
    return client.putObject(request, empty());
  }

  private static PutObjectRequest createFolderObjectRequest(final String bucket, final String key) {
    return PutObjectRequest.builder().bucket(bucket).key(key).build();
  }

  public static CompletionStage<DeleteObjectResponse> deleteObject(
      final String bucket, final String key) {
    return deleteObject(deleteObjectRequest(bucket, key));
  }

  public static CompletionStage<DeleteObjectResponse> deleteObject(
      final DeleteObjectRequest request) {
    return client.deleteObject(request);
  }

  public static DeleteObjectRequest deleteObjectRequest(final String bucket, final String key) {
    return DeleteObjectRequest.builder().bucket(bucket).key(key).build();
  }

  public static CompletionStage<HeadObjectResponse> headObject(
      final String bucket, final String key) {
    return headObject(headObjectRequest(bucket, key));
  }

  public static CompletionStage<HeadObjectResponse> headObject(final HeadObjectRequest request) {
    return client.headObject(request);
  }

  public static HeadObjectRequest headObjectRequest(final String bucket, final String key) {
    return HeadObjectRequest.builder().bucket(bucket).key(key).build();
  }

  public static CompletionStage<Pair<GetObjectResponse, Publisher<ByteBuffer>>> getObject(
      final String bucket, final String key) {
    return getObject(getObjectRequest(bucket, key));
  }

  public static CompletionStage<Pair<GetObjectResponse, Publisher<ByteBuffer>>> getObject(
      final GetObjectRequest request) {
    return client
        .getObject(request, new Response())
        .thenApply(pair -> pair(pair.first, toFlowPublisher(pair.second)));
  }

  public static GetObjectRequest getObjectRequest(final String bucket, final String key) {
    return GetObjectRequest.builder().bucket(bucket).key(key).build();
  }

  public static CompletionStage<PutObjectResponse> putObject(
      final String bucket,
      final String key,
      final String contentType,
      final long contentLength,
      final Publisher<ByteBuffer> content) {
    return putObject(putObjectRequest(bucket, key, contentType, contentLength), content);
  }

  public static CompletionStage<PutObjectResponse> putObject(
      final PutObjectRequest request, final Publisher<ByteBuffer> content) {
    return client.putObject(request, fromPublisher(toPublisher(content)));
  }

  public static PutObjectRequest putObjectRequest(
      final String bucket, final String key, final String contentType, final long contentLength) {
    return PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .contentType(contentType)
        .contentLength(contentLength)
        .build();
  }

  public static CompletionStage<Void> setMetadata(
      final String bucket, final String key, final Map<String, String> metadata) {
    return client.copyObject(copyObjectRequest(bucket, key, metadata)).thenAccept(r -> {});
  }

  private static class Response
      implements AsyncResponseTransformer<
          GetObjectResponse, Pair<GetObjectResponse, SdkPublisher<ByteBuffer>>> {
    private final CompletableFuture<Pair<GetObjectResponse, SdkPublisher<ByteBuffer>>> future =
        new CompletableFuture<>();
    private GetObjectResponse objectResponse;

    @Override
    public void exceptionOccurred(final Throwable error) {
      future.completeExceptionally(error);
    }

    @Override
    public void onResponse(final GetObjectResponse response) {
      this.objectResponse = response;

      if (response.sdkHttpResponse().statusCode() != 200) {
        future.complete(pair(objectResponse, fromIterable(list())));
      }
    }

    @Override
    public void onStream(final SdkPublisher<ByteBuffer> publisher) {
      if (!future.isDone()) {
        future.complete(pair(objectResponse, publisher));
      }
    }

    @Override
    public CompletableFuture<Pair<GetObjectResponse, SdkPublisher<ByteBuffer>>> prepare() {
      return future;
    }
  }
}

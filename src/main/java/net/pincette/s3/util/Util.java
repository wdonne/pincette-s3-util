package net.pincette.s3.util;

import static java.nio.channels.FileChannel.open;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.delete;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.ReadableByteChannelPublisher.readableByteChannel;
import static net.pincette.rs.Util.onComplete;
import static net.pincette.rs.Util.onCompleteProcessor;
import static net.pincette.rs.WritableByteChannelSubscriber.writableByteChannel;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToGetRethrow;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;
import static org.reactivestreams.FlowAdapters.toPublisher;
import static software.amazon.awssdk.core.async.AsyncRequestBody.empty;
import static software.amazon.awssdk.core.async.AsyncRequestBody.fromPublisher;
import static software.amazon.awssdk.core.async.SdkPublisher.fromIterable;
import static software.amazon.awssdk.services.s3.S3AsyncClient.create;
import static software.amazon.awssdk.services.s3.model.MetadataDirective.REPLACE;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow.Publisher;
import net.pincette.rs.Fanout;
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

  private static Pair<Long, Publisher<ByteBuffer>> failedBody() {
    return pair(-1L, net.pincette.rs.Util.empty());
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
    return Optional.of(contentLength)
        .filter(l -> l == -1)
        .map(l -> putObjectTemporarily(content))
        .orElseGet(() -> completedFuture(pair(contentLength, content)))
        .thenComposeAsync(
            pair -> putObject(putObjectRequest(bucket, key, contentType, pair.first), pair.second));
  }

  public static CompletionStage<PutObjectResponse> putObject(
      final PutObjectRequest request, final Publisher<ByteBuffer> content) {
    return client.putObject(request, fromPublisher(toPublisher(content)));
  }

  private static CompletionStage<Pair<Long, Publisher<ByteBuffer>>> putObjectTemporarily(
      final Publisher<ByteBuffer> content) {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    return tryToGetRethrow(() -> createTempFile(UUID.randomUUID().toString(), ".tmp"))
        .flatMap(
            path ->
                tryToGetRethrow(() -> open(path, WRITE, SYNC)).map(channel -> pair(path, channel)))
        .map(
            pair -> {
              content.subscribe(
                  Fanout.of(
                      writableByteChannel(pair.second), onComplete(() -> future.complete(null))));

              return (CompletionStage<Pair<Long, Publisher<ByteBuffer>>>)
                  future.thenApply(
                      v ->
                          tryToGetRethrow(() -> open(pair.first, READ))
                              .map(
                                  channel ->
                                      with(readableByteChannel(channel))
                                          .map(onCompleteProcessor(() -> delete(pair.first)))
                                          .get())
                              .map(publisher -> pair(pair.first.toFile().length(), publisher))
                              .orElseGet(Util::failedBody));
            })
        .orElseGet(() -> completedFuture(failedBody()));
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

module net.pincette.s3util {
  requires software.amazon.awssdk.services.s3;
  requires software.amazon.awssdk.core;
  requires software.amazon.awssdk.http;
  requires net.pincette.common;
  requires org.reactivestreams;

  exports net.pincette.s3.util;
}

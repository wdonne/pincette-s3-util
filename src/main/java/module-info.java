module net.pincette.s3util {
  requires software.amazon.awssdk.services.s3;
  requires software.amazon.awssdk.core;
  requires software.amazon.awssdk.http;
  requires org.reactivestreams;
  requires net.pincette.rs;
  requires net.pincette.common;

  exports net.pincette.s3.util;
}

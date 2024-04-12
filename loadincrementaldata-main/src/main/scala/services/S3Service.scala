package services

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import org.apache.commons.io.IOUtils

import java.io.IOException
import java.nio.charset.Charset

class S3Service {

  private var s3: AmazonS3 = _

  /**
   * Creates an S3 service with default parameters
   */
  def init(): Unit = {
    s3 = AmazonS3ClientBuilder.standard.withRegion(Regions.US_EAST_1).build
  }

  def uriFromPair(bucket: String, key: String): AmazonS3URI = {
    var formattedBucket: String = bucket
    if (bucket.endsWith("/")) {
      formattedBucket = bucket.substring(0, bucket.length - 1)
    }
    new AmazonS3URI("s3://" + formattedBucket + "/" + key)
  }

  def get(obj: AmazonS3URI): String = try {
    val downloadedObject = s3.getObject(obj.getBucket, obj.getKey)
    IOUtils.toString(downloadedObject.getObjectContent, Charset.defaultCharset)
  } catch {
    case ex: IOException =>
      throw new RuntimeException(ex)
  }

}

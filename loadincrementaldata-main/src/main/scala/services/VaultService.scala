package services

import org.apache.log4j.Logger

import java.io.{File, PrintWriter}
import scala.sys.process._
import scala.util.parsing.json.JSON

/**
 * Establishes connectivity between Vault & Databricks
 */
class VaultService {

  // From environment variables
  val env: String = System.getenv("ENV")
  val role: String = System.getenv("ROLE")
  val bucket: String = System.getenv("BUCKET")

  var cert: String = _ // Downloaded from S3
  var setupComplete: Boolean = false
  var token: String = _

  val path = s"https://vault.us-east-1.management.directsupply-${env}.cloud"

  val logger: Logger = Logger.getLogger(this.getClass)

  /**
   * Downloads the Direct Supply certificate from S3 bucket
   *
   * @return  Certificate, in String form
   */
  def downloadCertificate(): String = {
    val s3: S3Service = new S3Service()
    s3.init()
    return s3.get(s3.uriFromPair(bucket, "certs/directsupply-enterprise-root-ca.crt"))
  }

  /**
   *  Performs initial authentication with Vault by the following steps:
   *  - Download the DS Vault Certificate
   *  - Download the Vault CLI
   *  - Install the Vault CLI on local cluster
   *  - Update certificates via CLI
   *  - Perform Vault Login using Vault CLI and Databricks instance profile
   *  - Save the provided token
   */
  def authenticate(): Unit = {
    if(!setupComplete) {
      cert = downloadCertificate()
      ("wget https://releases.hashicorp.com/vault/1.11.3/vault_1.11.3_linux_amd64.zip").!!
      ("unzip vault_1.11.3_linux_amd64.zip").!!
      ("sudo mv vault /usr/bin").!!
      val writer: PrintWriter = new PrintWriter(new File("/usr/local/share/ca-certificates/directsupply-enterprise-root-ca2.crt"))
      writer.println(cert)
      writer.flush()
      writer.close()
      Process(Seq("bash", "-c", "update-ca-certificates")).!
      setupComplete = true
    }

    val vaultToken = new StringBuilder
    val vaultTokenErr = new StringBuilder
    Process(Seq("bash", "-c", "vault login -field=token -method=aws header_value=vault.management.directsupply.cloud role=" + role), None, "VAULT_ADDR" -> path) ! ProcessLogger(vaultToken append _, vaultTokenErr append _)

    token = vaultToken.toString
  }

  /**
   * Gets dynamic credentials for a desired server
   *
   * @param server  Server name to get credentials for
   * @return        Map containing "username" and "password" fields
   */
  def getServerCredentials(server: String): Map[String, String] = {
    val vaultData = new StringBuilder
    val vaultDataErr = new StringBuilder
    Process(Seq("bash", "-c", s"vault read -format=json databases/creds/${server}-DSIAPP_DatabricksReplication"),
      None, "VAULT_ADDR" -> path, "VAULT_TOKEN" -> token) ! ProcessLogger(vaultData append _, vaultDataErr append _)

    val result = JSON.parseFull(vaultData.toString)
    var vaultUsername: String = ""
    var vaultPassword: String = ""
    result match {
      case Some(map: Map[String, Any]) =>
        vaultUsername = map.get("data").get.asInstanceOf[Map[String,String]].get("username").get
        vaultPassword = map.get("data").get.asInstanceOf[Map[String,String]].get("password").get
      case None => throw new IllegalArgumentException("Parsing failed of Vault return")
      case other => throw new IllegalArgumentException("Unknown data structure from Vault return: " + other)
    }

    Map("username" -> vaultUsername, "password" -> vaultPassword)
  }
}

/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ceph.rgw

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.Progressable
import org.apache.log4j.Logger
import CephStoreUtil.{checkRootPath, getCompletePath, getObjectPath, getSuffixes}
import org.javaswift.joss.client.factory.{AccountFactory, AuthenticationMethod, AuthenticationMethodScope}
import org.javaswift.joss.model.Account

import java.io.{File, FileNotFoundException, IOException}
import java.net.URI
import java.util
import scala.util.control.Breaks
import scala.util.control.Breaks.{break, breakable}


/**
 * Implementation of FileSystem for Ceph RGW ,
 * used to access Ceph RGW system in a filesystem style.
 */
class CephStoreSystem extends FileSystem {
  val LOG = Logger.getLogger(this.getClass)
  private val FS_CEPH_USERNAME = "spark.hadoop.fs.ceph.username"
  private val FS_CEPH_PASSWORD = "spark.hadoop.fs.ceph.password"
  private val FS_CEPH_URI = "spark.hadoop.fs.ceph.uri"
  private val AUTH_URI = "spark.hadoop.fs.auth.uri"
  private val TENANT_NAME = "spark.hadoop.fs.tenant.name"
  private val TENANT_ID = "spark.hadoop.fs.tenant.id"
  private val DOMAIN_NAME = "spark.hadoop.fs.domain.name"
  private val AUTH_METHOD = "spark.hadoop.fs.auth.method"
  private var account: Account = null
  private var bucketName: String = null
  private var uri: URI = null
  private val workingDir: Path = null
  private var username: String = null


  override def getScheme(): String = {
    val scheme = "ceph"
    scheme
  }

  /**
   * Initialize new FileSystem.
   * the uri of the file system, including host, port, etc.
   *
   * @param conf configuration of the file system
   * @throws IOException IO problems
   */
  override def initialize(uri: URI, conf: Configuration): Unit = {
    this.setConf(conf)
    checkRootPath(uri)
    this.bucketName = uri.getHost
    this.username = UserGroupInformation.getCurrentUser.getShortUserName
    this.uri = uri
    this.account = getAccount
    super.initialize(uri, conf)
  }

  private def getAccount = {
    val conf = getConf
    try {
      if (conf.get(AUTH_METHOD) == "keystone") {
        new AccountFactory()
          .setUsername(conf.get(FS_CEPH_USERNAME))
          .setPassword(conf.get(FS_CEPH_PASSWORD))
          .setAuthenticationMethod(AuthenticationMethod.KEYSTONE_V3)
          .setAuthUrl(conf.get(AUTH_URI))
          .setTenantName(conf.get(TENANT_NAME))
          .setTenantId(conf.get(TENANT_ID))
          .setDomain(conf.get(DOMAIN_NAME))
          .setAuthenticationMode(AuthenticationMethodScope.PROJECT_NAME)
          .createAccount
      } else { 
        new AccountFactory().setUsername(
        conf.get(FS_CEPH_USERNAME)).setPassword(
        conf.get(FS_CEPH_PASSWORD)).setAuthenticationMethod(
        AuthenticationMethod.BASIC).setAuthUrl(conf.get(FS_CEPH_URI)).createAccount
      }
    } catch {
        case e: IOException =>
          e.printStackTrace()
          throw new IOException("Failed to create account! Please check your user information")
    }
  }

  override def getUri: URI = uri

  /**
   * Does the path represent to a directory ?
   *
   * @Returns: true if this is a directory.
   */
  def isDir(path: Path): Boolean = {
    val containerName = path.toUri.getHost
    val objectName = getObjectPath(path)
    val objects = account.getContainer(containerName).list(getSuffixes(objectName), "", 2)
    if (objects.size() > 0) return true
    false
  }

  override def open(path: Path, i: Int): FSDataInputStream = {
    val fileStatus = getFileStatus(path)
    if (fileStatus.isDirectory) {
      throw new FileNotFoundException("Can't open " + path + " because it is a directory")
    }
    val container = account.getContainer(bucketName)
    val storedObject = container.getObject(getObjectPath(path))
    val byte = storedObject.downloadObject()
    new FSDataInputStream(new CephStoreInputStream(byte))
  }

  @throws[IOException]
  override def create(path: Path, fsPermission: FsPermission,
                      overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long,
                      progress: Progressable): FSDataOutputStream = {
    val out = this.createCephObjectOutputStream(path)
    new FSDataOutputStream(out, this.statistics)
  }

  @throws[IOException]
  protected def createCephObjectOutputStream(path: Path) = {
    val partSizeKB = 1024
    new CephStoreOutputStream(this.getConf, account, path.toUri.toString, partSizeKB, bucketName)
  }

  @throws[IOException]
  override def append(f: Path, bufferSize: Int, progress: Progressable): Nothing = {
    throw new IOException("Append is not supported!")
  }

  override def rename(srcPath: Path, dstPath: Path): Boolean = {
    if (srcPath.isRoot) { // Cannot rename root of file system
      if (LOG.isDebugEnabled) LOG.debug("Cannot rename the root of a filesystem")
      return false
    }
    var parent = dstPath.getParent
    while (parent != null && !srcPath.equals(parent)) {
      parent = parent.getParent
    }
    if (parent != null) return false

    val srcStatus = getFileStatus(srcPath)
    var dstStatus: FileStatus = null
    try {
      dstStatus = getFileStatus(dstPath)
    } catch {
      case fnde: FileNotFoundException =>
        dstStatus = null
    }
    if (dstStatus == null) // If dst doesn't exist, check whether dst dir exists or not
      dstStatus = getFileStatus(dstPath.getParent)
    else {

    }
    var succeed: Boolean = true
    if (srcStatus.isDirectory) {
      succeed = copyDirectory(srcPath, dstPath)
    }
    else {
      succeed = copyFile(srcPath, srcStatus.getLen, dstPath)
    }
    srcPath == dstPath || succeed
  }

  private def copyDirectory(srcPath: Path, dstPath: Path): Boolean = {
    val srcKey = srcPath.toUri.getPath
    val dstKey = dstPath.toUri.getPath
    val listObject = account.getContainer(bucketName).list(srcKey.substring(1), "", 100)
    listObject.forEach(storeObject => {
      val dstObject = storeObject.getName.replace(srcKey.substring(1), dstKey)
      val inputStream = storeObject.downloadObjectAsInputStream()
      account.getContainer(bucketName).getObject(dstObject.substring(1)).uploadObject(inputStream)
      storeObject.delete()
    })
    true
  }

  private def copyFile(srcPath: Path, srcLen: Long, dstPath: Path): Boolean = {
    val srcKey = srcPath.toUri.getPath
    val dstKey = dstPath.toUri.getPath
    val inputStream = account.getContainer(bucketName).getObject(srcKey.substring(1)).downloadObjectAsInputStream()
    val bareName = new File(srcPath.toUri.getPath).getName
    val path = dstKey + "/" + bareName
    account.getContainer(bucketName).getObject(path.substring(1)).uploadObject(inputStream)
    account.getContainer(bucketName).getObject(srcKey.substring(1)).delete()
    true
  }

  override def delete(path: Path, recursive: Boolean): Boolean = {
    try {
      val container = account.getContainer(bucketName)
      val objectName = getObjectPath(path)
      val storeObject = container.getObject(objectName)
      if (storeObject.exists) storeObject.delete()
      return true
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        LOG.error("Failed to delete :" + path)

    }
    false

  }

  def pathToKey(path: Path): String = {
    var pathFinal = path
    if (!path.isAbsolute) pathFinal = new Path(workingDir.toUri.toString, path.toString)
    if (path.toUri.getScheme != null && path.toUri.getPath.isEmpty) return ""
    getObjectPath(pathFinal)
  }

  def qualify(path: Path): Path = path.makeQualified(uri, workingDir)

  override def listStatus(f: Path): Array[FileStatus] = {
    val path = qualify(f)
    var key = pathToKey(path)
    val fileStatus = getFileStatus(path)
    if (fileStatus.isDirectory) if (!key.isEmpty) key = key + '/'
    val containerSize = account.getContainer(f.toUri.getHost).list().size()
    val objectsList = account.getContainer(bucketName)
      .list(getObjectPath(f), "", containerSize)
    val cephObjectStatuses = new util.ArrayList[FileStatus]
    objectsList.forEach(storeObject1 => {
      if (storeObject1.getName.equals(key + "/")) {
        LOG.debug("Ignoring: " + storeObject1.getName)
      } else {
        val completePath = "ceph://" + f.toUri.getHost + "/" + storeObject1.getName
        val hdfsPath = new Path(completePath)
        val objectStatus = getFileStatus(hdfsPath)
        cephObjectStatuses.add(objectStatus)
      }
    })
    cephObjectStatuses.toArray(new Array[FileStatus](0))
  }


  override def setWorkingDirectory(new_dir: fs.Path): Unit = {
    throw new IOException("Set working directory is not supported!")
  }

  override def getWorkingDirectory(): Path = {
    new Path(uri.toString)
  }

  override def mkdirs(path: Path, fsPermission: FsPermission): Boolean = {
    var fileStatus: FileStatus = null
    var temPath: Path = null
    val qualifyPath = qualify(path)
    try {
      fileStatus = getFileStatus(path)
      if (fileStatus.isDirectory) return true
      else throw new FileAlreadyExistsException("Path is a file: " + path)
    } catch {
      case e: FileNotFoundException =>
        temPath = path.getParent
        val loop = new Breaks
        loop.breakable {
          while (temPath != null) {
            try {
              fileStatus = getFileStatus(temPath)
              if (fileStatus.isDirectory) {
                loop.break
              }
              if (fileStatus.isFile) {
                throw new FileAlreadyExistsException(
                  String.format("Can't make directory for path '%s' since it is a file.", temPath))
              }
            }
            catch {
              case e: FileNotFoundException =>
                temPath = temPath.getParent

            }
          }
        }

    }
    val objectName = getSuffixes(path.toUri.getHost) + pathToKey(qualifyPath)
    val postfix = objectName.endsWith("/")
    if (!postfix) account.getContainer(getSuffixes(objectName)).create()
    else account.getContainer(objectName)
    true
  }

  override def getFileStatus(path: Path): FileStatus = {
    val qualifyPath = path.makeQualified(uri, workingDir)
    var LastModifiedAsTime: Long = 0
    val objectPath = getObjectPath(qualifyPath)
    if (objectPath.length == 0) return new CephStoreStatus(LastModifiedAsTime, qualifyPath, username)
    val objectsList = account.getContainer(bucketName).list(objectPath, "", 2)
    if (objectsList.size() < 1) {
      throw new FileNotFoundException("No such file or directory: " + qualifyPath)
    }
    if (isDir(path)) {
      objectsList.forEach(objects => {
        breakable {
          LastModifiedAsTime = objects.getLastModifiedAsDate.getTime
          break
        }
      })
      return new CephStoreStatus(LastModifiedAsTime, qualifyPath, username)
    }
    LastModifiedAsTime = account.getContainer(bucketName)
      .getObject(objectPath).getLastModifiedAsDate.getTime
    val contentLength = account.getContainer(bucketName).getObject(objectPath).getContentLength
    new CephStoreStatus(contentLength, LastModifiedAsTime,
      qualifyPath, getDefaultBlockSize(qualifyPath), username)
  }
}

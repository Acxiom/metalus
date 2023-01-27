package com.acxiom.metalus.context

import com.acxiom.metalus.audits.ExecutionAudit
import com.acxiom.metalus.utils.ReflectionUtils
import com.acxiom.metalus.{ClassInfo, CredentialProvider, PipelineStateInfo, PipelineStepResponse, UserNameCredential}
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.sql.DriverManager
import java.util.{Base64, Date, Properties, UUID}
import scala.jdk.CollectionConverters._

trait SessionContext extends Context {
  def existingSessionId: Option[String]

  def sessionStorage: Option[ClassInfo]

  def sessionConvertors: Option[List[ClassInfo]]

  def credentialProvider: Option[CredentialProvider]

  /**
   * Saves the result of the step.
   *
   * @param key    The step key.
   * @param result The result to save.
   * @return true if the step could be saved.
   */
  def saveStepResult(key: PipelineStateInfo, result: PipelineStepResponse): Boolean

  /**
   * Loads the step results for this session or None if this is a new session or has no recorded results.
   *
   * @return An optional list of step results.
   */
  def loadStepResults(): Option[List[PipelineStepResponse]]

  /**
   * Saves an audit for the provided key.
   *
   * @param key   The key
   * @param audit The audit to store
   * @return True if the audit was saved.
   */
  def saveAudit(key: PipelineStateInfo, audit: ExecutionAudit): Boolean

  /**
   * Loads the audits for this session or None if this is a new session or has no recorded audits.
   *
   * @return An optional list of audits
   */
  def loadAudits(): Option[List[ExecutionAudit]]

  /**
   * Stores the globals for this session.
   *
   * @param key The pipeline key for these globals
   * @param globals The globals to store.
   * @return True if the globals could be stored.
   */
  def saveGlobals(key: PipelineStateInfo, globals: Map[String, Any]): Boolean

  /**
   * Loads the globals for this session or None if none were recorded.
   *
   * @param key The pipeline key for these globals
   * @return An optional globals map.
   */
  def loadGlobals(key: PipelineStateInfo): Option[Map[String, Any]]

  /**
   * Saves the status identified by the provided key. Valid status codes are:
   * RUNNING
   * COMPLETE
   * ERROR
   *
   * @param key    The unique key for the status
   * @param status The status code
   * @return True if the status could be saved.
   */
  def saveStepStatus(key: PipelineStateInfo, status: String): Boolean
}

/* TODO [2.0 Review]
 * Should we include the PipelineContext in these calls or would the ContextManager be enough?
 *
 * Save step status in execute step
 *  Set failed status in the processPipelineStep try/catch after the retries have been exhausted in processStepWithRetry
 *  If a step fails and has nextStepOnError, set the status to ERROR and proceed in processStepWithRetry
 * Save result in execute step
 * Save globals in the execute step
 *  Only save globals at the beginning of a pipeline (and store that id) and when changes are made
 *  Forks will do a lot of overwrites
 *  Should we track the version so it doesn't increment after every step?
 *  Must update after each step since steps can add globals
 *  Should we add fork information so we can have different copies for each fork?
 *    If we allow a fork to be restarted, then we will need this state.
 *
 *
 * Since these are "global", how do we handle the version?
 * Can we determine if a global has changed?
 *  Calling only when globals are updated will help, but the entire map is being sent
 *    What about creating a map of changes and sending that?
 *  Anyway to perform a diff?
 *    Compare the state? Since it is stored as a string, we could try ==
 *
 */
/**
 * The session context is used to manage state of a running flow.
 *
 * @param existingSessionId  An optional sessionId used during recovery or restarts.
 * @param sessionStorage     The information needed to load a SessionStorage object.
 * @param sessionConvertors  The information needed to load additional state convertors.
 * @param credentialProvider Optional credential provider to assist with authentication
 */
case class DefaultSessionContext(override val existingSessionId: Option[String],
                                 override val sessionStorage: Option[ClassInfo],
                                 override val sessionConvertors: Option[List[ClassInfo]],
                                 override val credentialProvider: Option[CredentialProvider] = None) extends SessionContext {
  private val logger = LoggerFactory.getLogger(DefaultSessionContext.getClass)

  private val defaultConvertor = DefaultSessionConvertor()

  private val primaryKey = "primaryKey"

  private lazy val storage: SessionStorage = {
    if (sessionStorage.isDefined) {
      ReflectionUtils.loadClass(sessionStorage.get.className.getOrElse("com.acxiom.metalus.context.NoopSessionStorage"),
        Some(sessionStorage.get.parameters.getOrElse(Map()) + ("credentialProvider" -> credentialProvider))).asInstanceOf[SessionStorage]
    } else {
      NoopSessionStorage()
    }
  }

  private lazy val convertors = {
    if (sessionConvertors.isDefined && sessionConvertors.get.nonEmpty) {
      sessionConvertors.get.map(info => {
        ReflectionUtils.loadClass(info.className.get, info.parameters).asInstanceOf[SessionConvertor]
      }) :+ defaultConvertor
    } else {
      List(defaultConvertor)
    }
  }
  /**
   * The unique session id being used to track state.
   *
   * @return The session id
   */
  val sessionId: UUID = {
    if (existingSessionId.isDefined) {
      UUID.fromString(existingSessionId.get)
    } else {
      UUID.randomUUID()
    }
  }

  /**
   * Saves the result of the step.
   *
   * @param key    The step key.
   * @param result The result to save.
   * @return true if the step could be saved.
   */
  def saveStepResult(key: PipelineStateInfo, result: PipelineStepResponse): Boolean = {
    val saved = saveStepResult(result.primaryReturn.getOrElse(""), key.key, primaryKey)
    if (result.namedReturns.isDefined) {
      result.namedReturns.get.foldLeft(saved)((s, r) => {
        if (saveStepResult(r._2, key.key, r._1) && s) {
          true
        } else {
          false
        }
      })
    } else {
      saved
    }
  }

  /**
   * Loads the step results for this session or None if this is a new session or has no recorded results.
   *
   * @return An optional list of step results.
   */
  def loadStepResults(): Option[List[PipelineStepResponse]] = {
    val results = storage.loadStepResults(sessionId)
    if (results.isDefined && results.get.nonEmpty) {
      val resultGroups = results.get.groupBy(record => (record.resultKey, record.name)).map(group => {
        if (group._2.length == 1) {
          group._2.head
        } else {
          group._2.maxBy(_.version.getOrElse(-1))
        }
      })
      val resultsMap = resultGroups.foldLeft(Map[String, PipelineStepResponse]())((responseMap, result) => {
        val response = if (responseMap.contains(result.resultKey)) {
          responseMap(result.resultKey)
        } else {
          PipelineStepResponse(None, None)
        }
        val convertor: SessionConvertor = findNamedConvertor(result.convertor)
        val updatedResponse = if (result.name == primaryKey) {
          response.copy(primaryReturn = Some(convertor.deserialize(result.state)))
        } else { // Secondary return
          val map = if (response.namedReturns.isDefined) {
            response.namedReturns.get
          } else {
            Map[String, Any]()
          }
          response.copy(namedReturns = Some(map + (result.name -> convertor.deserialize(result.state))))
        }
        responseMap + (result.resultKey -> updatedResponse)
      })
      Some(resultsMap.values.toList)
    } else {
      None
    }
  }

  /**
   * Saves an audit for the provided key.
   *
   * @param key   The key
   * @param audit The audit to store
   * @return True if the audit was saved.
   */
  def saveAudit(key: PipelineStateInfo, audit: ExecutionAudit): Boolean = {
    val convertor = convertors.find(_.canConvert(audit))
    if (convertor.isDefined) {
      storage.saveAudit(AuditSessionRecord(sessionId, new Date(), None, convertor.get.serialize(audit),
        convertor.get.name, key.key, audit.start, audit.end.getOrElse(-1L), audit.durationMs.getOrElse(-1L)))
    } else {
      logger.warn(s"Unable to serialize object for key: ${key.key}")
      false
    }
  }

  /**
   * Loads the audits for this session or None if this is a new session or has no recorded audits.
   *
   * @return An optional list of audits
   */
  def loadAudits(): Option[List[ExecutionAudit]] = {
    val audits = storage.loadAudits(sessionId)
    if (audits.isDefined) {
      Some(audits.get.groupBy(_.auditKey).map(group => {
        val record = if (group._2.length == 1) {
          group._2.head
        } else {
          group._2.maxBy(_.version.getOrElse(-1))
        }
        val convertor: SessionConvertor = findNamedConvertor(record.convertor)
        convertor.deserialize(record.state).asInstanceOf[ExecutionAudit]
      }).toList)
    } else {
      None
    }
  }

  /**
   * Stores the globals for this session.
   *
   * @param globals The globals to store.
   * @return True if the globals could be stored.
   */
  def saveGlobals(key: PipelineStateInfo, globals: Map[String, Any]): Boolean = {
    globals.forall(global => {
      val convertor = convertors.find(_.canConvert(global._2))
      if (convertor.isDefined) {
        storage.saveGlobal(GlobalSessionRecord(sessionId, new Date(), None, convertor.get.serialize(global._2),
          convertor.get.name, key.key, global._1))
      } else {
        false
      }
    })
  }

  /**
   * Loads the globals for this session or None if none were recorded.
   *
   * @return An optional globals map.
   */
  def loadGlobals(key: PipelineStateInfo): Option[Map[String, Any]] = {
    // TODO [2.0 Review] Since this will pull global maps for *all* pipelines, do we need to provide the pipeline key?
    val globalRecords = storage.loadGlobals(sessionId)
    if (globalRecords.isDefined) {
//      globalRecords.get.groupBy(_.)
      Some(globalRecords.get.foldLeft(Map[String, Any]())((mapResponse, global) => {
        val convertor = findNamedConvertor(global.convertor)
        mapResponse + (global.globalName -> convertor.deserialize(global.state))
      }))
    } else {
      None
    }
  }

  /**
   * Saves the status identified by the provided key. Valid status codes are:
   * RUNNING
   * COMPLETE
   * ERROR
   *
   * @param key    The unique key for the status
   * @param status The status code
   * @return True if the status could be saved.
   */
  def saveStepStatus(key: PipelineStateInfo, status: String): Boolean = {
    val convertedStatus = status.toUpperCase match {
      case "RUNNING" => "RUNNING"
      case "COMPLETE" => "COMPLETE"
      case "ERROR" => "ERROR"
      case _ => "UNKNOWN"
    }
    storage.setStatus(StatusSessionRecord(sessionId, new Date(), None, key.key, convertedStatus))
  }

  private def findNamedConvertor(convertorName: String) = {
    val convertorOpt = convertors.find(_.name == convertorName)
    val convertor = if (convertorOpt.isDefined) {
      convertorOpt.get
    } else {
      defaultConvertor
    }
    convertor
  }

  private def saveStepResult(obj: Any, key: String, name: String): Boolean = {
    val convertor = convertors.find(_.canConvert(obj))
    if (convertor.isDefined) {
      storage.saveStepResult(StepResultSessionRecord(sessionId, new Date(), None,
        convertor.get.serialize(obj), convertor.get.name, key, name))
    } else {
      logger.warn(s"Unable to serialize object for key: $key")
      false
    }
  }
}

/**
 * The SessionConvertor provides the methods used by the SessionContext to serialize/deserialize objects.
 */
trait SessionConvertor {
  /**
   * Provides a unique name that is used to locate this convertor from the list ov convertors.
   *
   * @return The unique name of this convertor.
   */
  def name: String

  /**
   * This method will inspect the object and determine if it should handle the serialization/deserialization. This
   * is used by custom convertors to indicate that this convertor should be used.
   *
   * @param obj The object to serialize/deserialize
   * @return true if this convertor should be used for serialization/deserialization.
   */
  def canConvert(obj: Any): Boolean

  /**
   * Serializes the provided object for storage. Implementations should override this for objects that cannot
   * be serialize as is and may need to be modified in a way to enable restoration later.
   *
   * @param obj The object to serialize.
   * @return A byte array that can be stored.
   */
  def serialize(obj: Any): Array[Byte]

  /**
   * Deserializes the provided object from storage. Implementations should override this for objects that cannot
   * be deserialized into the original object.
   *
   * @param obj The byte array to deserialize.
   * @return A byte array that can be deserialized.
   */
  def deserialize(obj: Array[Byte]): Any
}

/**
 * The default implementation of the SessionConvertor. This implementation uses Java serialization
 * to serialize/deserialize objects.
 */
case class DefaultSessionConvertor() extends SessionConvertor {
  override def name: String = "DefaultConvertor"

  override def canConvert(obj: Any): Boolean = obj.isInstanceOf[java.io.Serializable]

  override def serialize(obj: Any): Array[Byte] = {
    val o = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(o)
    out.writeObject(obj)
    out.flush()
    val result = o.toByteArray
    out.close()
    result
  }

  override def deserialize(obj: Array[Byte]): Any = {
    val input = new ByteArrayInputStream(obj)
    val stream = new ObjectInputStream(input)
    val newObj = stream.readObject()
    stream.close()
    newObj
  }
}

/**
 * Defines the minimum properties required to store state information from the SessionContext.
 */
trait SessionRecord {
  /**
   * The unique id for this session.
   *
   * @return A unique session id.
   */
  def sessionId: UUID

  /**
   * The date this record is to be stored.
   *
   * @return The date this recored was stored.
   */
  def date: Date

  /**
   * An id indicating the version within this session for the data. This is useful for restarts and recovery.
   *
   * @return An id indicating the version within this session for the data
   */
  def version: Option[Int]
}

/**
 * This trait extends the SessionRecord and adds the name of the convertor that is used for
 * serialization/deserialization and the serialized data to be stored.
 */
trait SerializedSessionRecord extends SessionRecord {
  /**
   * The name of the convertor to use for serialization/deserialization.
   *
   * @return The name of the convertor to use for serialization/deserialization.
   */
  def convertor: String


  /**
   * The serialized state data.
   *
   * @return The serialized state data.
   */
  def state: Array[Byte]

  /**
   * Converts the serialized state into a string which can be save.
   * @return A string representing the state.
   */
  def encodedState: String = Base64.getEncoder.encodeToString(state)
}

case class StatusSessionRecord(override val sessionId: UUID,
                               override val date: Date,
                               override val version: Option[Int],
                               resultKey: String,
                               status: String) extends SessionRecord

case class AuditSessionRecord(override val sessionId: UUID,
                              override val date: Date,
                              override val version: Option[Int],
                              override val state: Array[Byte],
                              override val convertor: String,
                              auditKey: String,
                              start: Long,
                              end: Long,
                              duration: Long) extends SerializedSessionRecord

case class StepResultSessionRecord(override val sessionId: UUID,
                                   override val date: Date,
                                   override val version: Option[Int],
                                   override val state: Array[Byte],
                                   override val convertor: String,
                                   resultKey: String,
                                   name: String) extends SerializedSessionRecord

case class GlobalSessionRecord(override val sessionId: UUID,
                               override val date: Date,
                               override val version: Option[Int],
                               override val state: Array[Byte],
                               override val convertor: String,
                               resultKey: String,
                               globalName: String) extends SerializedSessionRecord

/*
  * Possible schemas:
  * globals --> |sessionId|date|version|convertor|globalName|state
 */
trait SessionStorage {
  /**
   * Sets the current status for the provided key.
   *
   * @param sessionRecord The record containing the status data.
   * @return true if the status can be saved.
   */
  def setStatus(sessionRecord: StatusSessionRecord): Boolean

  /**
   * Stores audit data.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if the data can be stored.
   */
  def saveAudit(sessionRecord: AuditSessionRecord): Boolean

  /**
   * Loads the most recent version of the audits for this session.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  def loadAudits(sessionId: UUID): Option[List[AuditSessionRecord]]

  /**
   * Saves a step result.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if it is saved.
   */
  def saveStepResult(sessionRecord: StepResultSessionRecord): Boolean

  /**
   * Loads a list of step result records.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  def loadStepResults(sessionId: UUID): Option[List[StepResultSessionRecord]]

  /**
   * Saves an element of the globals.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if it is saved.
   */
  def saveGlobal(sessionRecord: GlobalSessionRecord): Boolean

  /**
   * Loads a list of global items.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  def loadGlobals(sessionId: UUID): Option[List[GlobalSessionRecord]]
}

case class NoopSessionStorage() extends SessionStorage {

  /**
   * Sets the current status for the provided key.
   *
   * @param sessionRecord The record containing the status data.
   * @return true if the status can be saved.
   */
  override def setStatus(sessionRecord: StatusSessionRecord): Boolean = true

  /**
   * Saves a step result.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if it is saved.
   */
  override def saveStepResult(sessionRecord: StepResultSessionRecord): Boolean = true

  /**
   * Loads a list of step result records.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  override def loadStepResults(sessionId: UUID): Option[List[StepResultSessionRecord]] = None

  /**
   * Saves an element of the globals.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if it is saved.
   */
  override def saveGlobal(sessionRecord: GlobalSessionRecord): Boolean = true

  /**
   * Loads a list of global items.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  override def loadGlobals(sessionId: UUID): Option[List[GlobalSessionRecord]] = None

  /**
   * Stores audit data.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if the data can be stored.
   */
  override def saveAudit(sessionRecord: AuditSessionRecord): Boolean = true

  /**
   * Loads the most recent version of the audits for this session.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  override def loadAudits(sessionId: UUID): Option[List[AuditSessionRecord]] = None
}

// noinspection SqlNoDataSourceInspection
case class JDBCSessionStorage(connectionString: String,
                              connectionProperties: Map[String, String],
                              credentialName: Option[String] = None,
                              credentialProvider: Option[CredentialProvider] = None) extends SessionStorage {
  private val p = new Properties()
  p.putAll(connectionProperties.asJava)

  private val CONN_PROPERTIES = if (credentialName.isDefined && credentialProvider.isDefined) {
    val cred = credentialProvider.get.getNamedCredential(credentialName.get)
    if (cred.isDefined) {
      p.setProperty("user", cred.get.asInstanceOf[UserNameCredential].username)
      p.setProperty("password", cred.get.asInstanceOf[UserNameCredential].password)
    }
    p
  } else {
    p
  }

  private val connection = DriverManager.getConnection(connectionString, CONN_PROPERTIES)

  /**
   * Sets the current status for the provided key.
   *
   * @param sessionRecord The record containing the status data.
   * @return true if the status can be saved.
   */
  override def setStatus(sessionRecord: StatusSessionRecord): Boolean = {
    // Table --> |SESSION_ID|DATE|VERSION|RESULT_KEY|STATUS
    val sharedWhere = s"where SESSION_ID = '${sessionRecord.sessionId}' AND RESULT_KEY = '${sessionRecord.resultKey}'"
    val results = connection.prepareStatement(s"select * from STEP_STATUS $sharedWhere").executeQuery()
    if (results.first()) {
      val count =
        connection.prepareStatement(s"update STEP_STATUS set STATUS = '${sessionRecord.status}' $sharedWhere ")
          .executeUpdate()
      count == 1
    } else {
      val count = connection.prepareStatement(
        s"""INSERT INTO STEP_STATUS
      VALUES('${sessionRecord.sessionId}', ${sessionRecord.date.getTime},
      ${sessionRecord.version.getOrElse(0)}, '${sessionRecord.resultKey}',
      '${sessionRecord.status}')""").executeUpdate()
      count == 1
    }
  }

  /**
   * Stores audit data.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if the data can be stored.
   */
  override def saveAudit(sessionRecord: AuditSessionRecord): Boolean = {
    // Table --> |SESSION_ID|DATE|VERSION|CONVERTOR|AUDIT_KEY|START|END|DURATION|STATE
    val sharedWhere = s"WHERE SESSION_ID = '${sessionRecord.sessionId}' AND AUDIT_KEY = '${sessionRecord.auditKey}'"
    val results = connection.prepareStatement(s"SELECT * FROM AUDITS $sharedWhere").executeQuery()
    if (results.first()) {
      // Increment the version here
      val version = if (results.getLong("END") == -1) {
        results.getInt("VERSION")
      } else {
        results.getInt("VERSION") + 1
      }
      val setClause =
        s"""VERSION = '$version', STATE = '${sessionRecord.encodedState}', END = ${sessionRecord.end},
           |START = ${sessionRecord.start}, DURATION = ${sessionRecord.duration},
           |DATE = ${sessionRecord.date.getTime}""".stripMargin
      val count = connection.prepareStatement(s"update AUDITS set $setClause $sharedWhere ").executeUpdate()
      count == 1
    } else {
      val valuesClause =s"""'${sessionRecord.sessionId}', '${sessionRecord.date.getTime}',
           |${sessionRecord.version.getOrElse(0)}, '${sessionRecord.convertor}',
           |'${sessionRecord.auditKey}', ${sessionRecord.start}, ${sessionRecord.end},
           |${sessionRecord.duration}, '${sessionRecord.encodedState}'""".stripMargin
      val count = connection.prepareStatement(s"INSERT INTO AUDITS VALUES($valuesClause)").executeUpdate()
      count == 1
    }
  }

  /**
   * Loads the most recent version of the audits for this session.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  override def loadAudits(sessionId: UUID): Option[List[AuditSessionRecord]] = {
    // Table --> |SESSION_ID|DATE|VERSION|CONVERTOR|AUDIT_KEY|START|END|DURATION|STATE
    val sharedWhere = s"WHERE SESSION_ID = '${sessionId.toString}'"
    val results = connection.prepareStatement(s"SELECT * FROM AUDITS $sharedWhere").executeQuery()
    val list = Iterator.from(0).takeWhile(_ => results.next()).map(_ => {
      AuditSessionRecord(UUID.fromString(results.getString("SESSION_ID")),
        new Date(results.getLong("DATE")),
        Some(results.getInt("VERSION")),
        Base64.getDecoder.decode(results.getString("STATE")),
        results.getString("CONVERTOR"),
        results.getString("AUDIT_KEY"),
        results.getLong("START"),
        results.getLong("END"),
        results.getLong("DURATION"))
    }).toList
    if (list.nonEmpty) {
      Some(list)
    } else {
      None
    }
  }

  /**
   * Saves a step result.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if it is saved.
   */
  override def saveStepResult(sessionRecord: StepResultSessionRecord): Boolean = {
    // Table --> |SESSION_ID|DATE|VERSION|CONVERTOR|RESULT_KEY|NAME|STATE
    val sharedWhere = s"WHERE SESSION_ID = '${sessionRecord.sessionId}' AND RESULT_KEY = '${sessionRecord.resultKey}'"
    val results = connection.prepareStatement(s"SELECT * FROM STEP_RESULTS $sharedWhere").executeQuery()
    if (results.next()) {
      val version = results.getInt("VERSION") + 1
      val setClause = s"VERSION = '$version', STATE = '${sessionRecord.encodedState}', DATE = ${sessionRecord.date.getTime}"
      val count = connection.prepareStatement(s"update STEP_RESULTS set $setClause $sharedWhere ").executeUpdate()
      count == 1
    } else {
      val valuesClause =
        s"""'${sessionRecord.sessionId}', '${sessionRecord.date.getTime}',
           |${sessionRecord.version.getOrElse(0)}, '${sessionRecord.convertor}',
           |'${sessionRecord.resultKey}', '${sessionRecord.name}', '${sessionRecord.encodedState}'""".stripMargin
      val count = connection.prepareStatement(s"INSERT INTO STEP_RESULTS VALUES($valuesClause)").executeUpdate()
      count == 1
    }
  }

  /**
   * Loads a list of step result records.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  override def loadStepResults(sessionId: UUID): Option[List[StepResultSessionRecord]] = {
    // Table --> |SESSION_ID|DATE|VERSION|CONVERTOR|RESULT_KEY|NAME|STATE
    val sharedWhere = s"WHERE SESSION_ID = '${sessionId.toString}'"
    val results = connection.prepareStatement(s"SELECT * FROM STEP_RESULTS $sharedWhere").executeQuery()
    val list = Iterator.from(0).takeWhile(_ => results.next()).map(_ => {
      StepResultSessionRecord(UUID.fromString(results.getString("SESSION_ID")),
        new Date(results.getLong("DATE")),
        Some(results.getInt("VERSION")),
        Base64.getDecoder.decode(results.getString("STATE")),
        results.getString("CONVERTOR"),
        results.getString("RESULT_KEY"),
        results.getString("NAME"))
    }).toList
    if (list.nonEmpty) {
      Some(list)
    } else {
      None
    }
  }

  /**
   * Saves an element of the globals.
   *
   * @param sessionRecord The record containing the state data.
   * @return true if it is saved.
   */
  override def saveGlobal(sessionRecord: GlobalSessionRecord): Boolean = {
    // Table --> |SESSION_ID|DATE|VERSION|CONVERTOR|RESULT_KEY|NAME|STATE
    val sharedWhere =
      s"""WHERE SESSION_ID = '${sessionRecord.sessionId}'
         |AND RESULT_KEY = '${sessionRecord.resultKey}'
         |AND NAME = '${sessionRecord.globalName}'""".stripMargin
    val results = connection.prepareStatement(s"SELECT * FROM GLOBALS $sharedWhere").executeQuery()
    if (results.next()) {
      if (results.getString("STATE") == sessionRecord.encodedState) {
        true
      } else {
        val version = results.getInt("VERSION") + 1
        val setClause = s"VERSION = '$version', STATE = '${sessionRecord.encodedState}', DATE = ${sessionRecord.date.getTime}"
        val count = connection.prepareStatement(s"update GLOBALS set $setClause $sharedWhere ").executeUpdate()
        count == 1
      }
    } else {
      val valuesClause =
        s"""'${sessionRecord.sessionId}', '${sessionRecord.date.getTime}',
           |${sessionRecord.version.getOrElse(0)}, '${sessionRecord.convertor}',
           |'${sessionRecord.resultKey}', '${sessionRecord.globalName}', '${sessionRecord.encodedState}'""".stripMargin
      val count = connection.prepareStatement(s"INSERT INTO GLOBALS VALUES($valuesClause)").executeUpdate()
      count == 1
    }
  }

  /**
   * Loads a list of global items.
   *
   * @param sessionId The unique session id.
   * @return An optional list of session records.
   */
  override def loadGlobals(sessionId: UUID): Option[List[GlobalSessionRecord]] = {
    // Table --> |SESSION_ID|DATE|VERSION|CONVERTOR|RESULT_KEY|NAME|STATE
    val sharedWhere = s"WHERE SESSION_ID = '${sessionId.toString}'"
    val results = connection.prepareStatement(s"SELECT * FROM GLOBALS $sharedWhere").executeQuery()
    val list = Iterator.from(0).takeWhile(_ => results.next()).map(_ => {
      GlobalSessionRecord(UUID.fromString(results.getString("SESSION_ID")),
        new Date(results.getLong("DATE")),
        Some(results.getInt("VERSION")),
        Base64.getDecoder.decode(results.getString("STATE")),
        results.getString("CONVERTOR"),
        results.getString("RESULT_KEY"),
        results.getString("NAME"))
    }).toList
    if (list.nonEmpty) {
      Some(list)
    } else {
      None
    }
  }
}

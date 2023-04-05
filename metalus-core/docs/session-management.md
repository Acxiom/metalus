[Home](../readme.md)

# Session Management
Session Management allows state information to be stored during the execution of the application. Audits, step results,
step status and globals are stored and retrieved as each step and pipeline completes. This allows the flow engine to
seamlessly handle restarts and recovery of applications. Pipelines are required to specify the steps that can be
restarted. This mechanism is used to validate that a restart is allowed and to assist the recovery process when restarting
a failed application.

* [Session Context](#session-context)
  * [Default Session Context](#default-session-context)
* [Session Convertor](#session-convertor)
  * [Default Session Convertor](#default-session-convertor)
* [Session Storage](#session-storage)
  * [Noop Session Storage](#noop-session-storage)
  * [JDBC Session Storage](#jdbc-session-storage)

## Session Context
The session context interface defines the methods that will be used by the flow engine to manage the session data. Upon
application startup, a new session will be started. An existing session can be recovered by providing the _--existingSessionId_
flag containing the id which should be restored. When this occurs, a new _runId_ will be created to track restarts/recoveries.

Two traits are provided to assist with managing sessions:
* Session Convertor - Used to serialize/deserialize audits, step results and globals.
* Session Storage - Used to store the serialized representations of the session objects.
### Default Session Context
A standard implementation of the [Session Context](#session-context) interface exists which provides default behaviors.
This implementation will interact with the [Session Convertor](#session-storage) and [Session Storage](#session-storage)
interfaces. As the set methods are called, the list of convertors will be consulted to serialize any objects and the storage
will be called to handle storing session information. When loading a previous session, this implementation will attempt
to restore state from the latest _runId_ available.
## Session Convertor
The session convertor interface defines the methods required to serialize/deserialize objects. Each implementation should
specify a name that is unique as this will be tagged with the stored data to make it is easier to deserialize objects.
The _canConvert_ method will be called on each registered convertor until one can be found that will handle the object.
This is used only during serialization. The _serialize_ and _deserialize_ methods are responsible for handling conversion
of objects at runtime for interacting with the [Session Storage](#session-storage).
### Default Session Convertor
The default session convertor implements Java serialization. This convertor will always be added when using the 
[default session context](#default-session-context).
## Session Storage
The session storage interface defines the methods required to store session objects in the underlying infrastructure.
### Noop Session Storage
This session storage implementation stores nothing and is only used as the default. All calls made when using this
implementation will be lost and nothing can be recovered.
### JDBC Session Storage
An implementation of session storage that uses JDBC for storage.
#### Table DDL
##### Step Status
This table stores the status for each step and _runId_. The _STATUS_ value will be either:
* RUNNING
* COMPLETE
* ERROR
* UNKNOWN
```sql
CREATE TABLE STEP_STATUS
(SESSION_ID VARCHAR(64), DATE BIGINT, RUN_ID INTEGER, RESULT_KEY VARCHAR(2048), STATUS VARCHAR(15))
```
##### Step Status Steps
This table contains the steeps that will be called after this step.
```sql
CREATE TABLE STEP_STATUS_STEPS
(SESSION_ID VARCHAR(64), RUN_ID INTEGER, RESULT_KEY VARCHAR(2048), STEP_ID VARCHAR(2048))
```
##### Audits
Contains the audit data for the current session and run.
```sql
CREATE TABLE AUDITS
(SESSION_ID VARCHAR(64), DATE BIGINT, RUN_ID INTEGER, CONVERTOR VARCHAR(2048), AUDIT_KEY VARCHAR(2048),
START_TIME BIGINT, END_TIME BIGINT, DURATION BIGINT, STATE BLOB)
```
##### Step Results
Contains the step result data for the current session and run.
```sql
CREATE TABLE STEP_RESULTS
(SESSION_ID VARCHAR(64), DATE BIGINT, RUN_ID INTEGER, CONVERTOR VARCHAR(2048), RESULT_KEY VARCHAR(2048),
NAME VARCHAR(512), STATE BLOB)
```
##### Globals
Contains the global data for the current session and run.
```sql
CREATE TABLE GLOBALS
(SESSION_ID VARCHAR(64), DATE BIGINT, RUN_ID INTEGER, CONVERTOR VARCHAR(2048), RESULT_KEY VARCHAR(2048),
NAME VARCHAR(512), STATE BLOB)
```
##### Sessions
Contains the session run data.
```sql
CREATE TABLE SESSIONS
(SESSION_ID VARCHAR(64), RUN_ID INTEGER, STATUS VARCHAR(15), START_TIME BIGINT, END_TIME BIGINT, DURATION BIGINT)
```
##### Session History
Contains the session history across job runs.
```sql
CREATE TABLE SESSION_HISTORY
(SESSION_ID VARCHAR(64), RUN_ID INTEGER,  STATUS VARCHAR(15), START_TIME BIGINT, END_TIME BIGINT, DURATION BIGINT)
```

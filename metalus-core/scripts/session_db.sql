-- Tracks the status of each step being executed
CREATE TABLE STEP_STATUS
(
    SESSION_ID VARCHAR(64),
    DATE       BIGINT,
    RUN_ID     INTEGER,
    RESULT_KEY VARCHAR(2048),
    STATUS     VARCHAR(15)
);

-- SESSION_ID should be enough of an index since there shouldn't be that much history
CREATE INDEX stepStatusIdx ON SESSIONS (SESSION_ID);

-- Tracks which step(s) is being called next
CREATE TABLE STEP_STATUS_STEPS
(
    SESSION_ID VARCHAR(64),
    RUN_ID     INTEGER,
    RESULT_KEY VARCHAR(2048),
    STEP_ID    VARCHAR(2048),
    PRIMARY KEY (SESSION_ID, RUN_ID, RESULT_KEY)
);

CREATE INDEX stepStatusStepsIdx ON SESSIONS (SESSION_ID);

-- Tracks thee audits collected during execution
CREATE TABLE AUDITS
(
    SESSION_ID VARCHAR(64),
    DATE       BIGINT,
    RUN_ID     INTEGER,
    CONVERTOR  VARCHAR(2048),
    AUDIT_KEY  VARCHAR(2048),
    START_TIME BIGINT,
    END_TIME   BIGINT,
    DURATION   BIGINT,
    STATE      BLOB
);

-- SESSION_ID should be enough of an index since there shouldn't be that many audits
CREATE INDEX auditsIdx ON SESSIONS (SESSION_ID);

-- Tracks the results of each completed step
CREATE TABLE STEP_RESULTS
(
    SESSION_ID VARCHAR(64),
    DATE       BIGINT,
    RUN_ID     INTEGER,
    CONVERTOR  VARCHAR(2048),
    RESULT_KEY VARCHAR(2048),
    NAME       VARCHAR(512),
    STATE      BLOB
);

-- SESSION_ID should be enough of an index since there shouldn't be that many results
CREATE INDEX stepResultsIdx ON SESSIONS (SESSION_ID);

-- Tracks the globals
CREATE TABLE GLOBALS
(
    SESSION_ID VARCHAR(64),
    DATE       BIGINT,
    RUN_ID     INTEGER,
    CONVERTOR  VARCHAR(2048),
    RESULT_KEY VARCHAR(2048),
    NAME       VARCHAR(512),
    STATE      BLOB,
    PRIMARY KEY(SESSION_ID, RUN_ID, RESULT_KEY, NAME)
);

-- SESSION_ID to begin with, but may change over time
CREATE INDEX stepResultsIdx ON SESSIONS (SESSION_ID);

-- Tracks information about the session
CREATE TABLE SESSIONS
(
    SESSION_ID VARCHAR(64),
    RUN_ID     INTEGER,
    STATUS     VARCHAR(15),
    START_TIME BIGINT,
    END_TIME   BIGINT,
    DURATION   BIGINT
);

CREATE INDEX sessionIdx ON SESSIONS (SESSION_ID);

-- Tracks each run of a session
CREATE TABLE SESSION_HISTORY
(
    SESSION_ID VARCHAR(64),
    RUN_ID     INTEGER,
    STATUS     VARCHAR(15),
    START_TIME BIGINT,
    END_TIME   BIGINT,
    DURATION   BIGINT
);

CREATE INDEX sessionHistoryIdx ON SESSION_PROCESS_HISTORY (SESSION_ID);

-- Tracks information about the external process executing a session
CREATE TABLE SESSION_PROCESS
(
    SESSION_ID VARCHAR(64),
    PROCESS_ID INTEGER,
    AGENT_ID   VARCHAR(64),
    HOSTNAME   VARCHAR(1024),
    EXIT_CODE  INTEGER NULL,
    START_TIME BIGINT,
    END_TIME   BIGINT NULL
);

CREATE INDEX sessionProcessIdx ON SESSION_PROCESS (AGENT_ID, SESSION_ID);

-- Tracks history of each process used to run a session
CREATE TABLE SESSION_PROCESS_HISTORY
(
    SESSION_ID VARCHAR(64),
    PROCESS_ID INTEGER,
    AGENT_ID   VARCHAR(64),
    HOSTNAME   VARCHAR(1024),
    EXIT_CODE  INTEGER NULL,
    START_TIME BIGINT,
    END_TIME   BIGINT NULL
);

CREATE INDEX sessionProcessHistoryIdx ON SESSION_PROCESS_HISTORY (SESSION_ID);

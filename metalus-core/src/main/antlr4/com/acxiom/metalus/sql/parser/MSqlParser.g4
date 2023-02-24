/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is an adaptation of Presto's presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 grammar.
 */
parser grammar MSqlParser;

import MExprParser;

options { tokenVocab = MSqlLexer; caseInsensitive = true;}

//tokens {
//    DELIMITER
//}

singleStatement
    : statement EOF
    ;

standaloneExpression
    : expression EOF
    ;

standaloneRoutineBody
    : routineBody EOF
    ;

statement
    : query                                                            #statementDefault
//    | USE schema=identifier                                            #use
//    | USE catalog=identifier DOT schema=identifier                     #use
//    | CREATE SCHEMA (IF NOT EXISTS)? qualifiedName
//        (WITH properties)?                                             #createSchema
//    | ALTER SCHEMA qualifiedName RENAME TO identifier                  #renameSchema
    | CREATE TABLE (IF NOT EXISTS)? qualifiedName columnAliases?
        (COMMENT string)?
        (WITH properties)? AS (query | L_PAREN query R_PAREN)
        (WITH (NO)? DATA)?                                             #createTableAsSelect
//    | CREATE TABLE (IF NOT EXISTS)? qualifiedName
//        L_PAREN tableElement (COMMA tableElement)* R_PAREN
//         (COMMENT string)?
//         (WITH properties)?                                            #createTable
    | DROP dropType=(TABLE|VIEW) (IF EXISTS)? dataReference            #dropTable
    | INSERT INTO dataReference columnAliases? query                   #insertInto
    | UPDATE dataReference SET setExpression (COMMA setExpression)*
        where?                                                         #update
    | DELETE FROM dataReference where?                                 #delete
    | TRUNCATE TABLE dataReference                                     #truncateTable
    | CREATE (OR REPLACE)? VIEW dataReference AS query                 #createView
    | CALL qualifiedName L_PAREN (callArgument (COMMA callArgument)*)? R_PAREN   #call
    | EXECUTE identifier (USING expression (COMMA expression)*)?       #execute
    ;

// disable with support for now
query
//    :  with? queryNoWith
    : queryNoWith
    ;

with
    : WITH RECURSIVE? namedQuery (COMMA namedQuery)*
    ;

tableElement
    : columnDefinition
    | likeClause
    ;

columnDefinition
    : identifier type (NOT NULL)? (COMMENT string)? (WITH properties)?
    ;

likeClause
    : LIKE qualifiedName (optionType=(INCLUDING | EXCLUDING) PROPERTIES)?
    ;

properties
    : L_PAREN property (COMMA property)* R_PAREN
    ;

property
    : identifier EQ expression
    ;

sqlParameterDeclaration
    : identifier type
    ;

routineCharacteristics
    : routineCharacteristic*
    ;

routineCharacteristic
    : LANGUAGE language
    | determinism
    | nullCallClause
    ;

alterRoutineCharacteristics
    : alterRoutineCharacteristic*
    ;

alterRoutineCharacteristic
    : nullCallClause
    ;

routineBody
    : returnStatement
    | externalBodyReference
    ;

returnStatement
    : RETURN expression
    ;

externalBodyReference
    : EXTERNAL (NAME externalRoutineName)?
    ;

language
    : SQL
    | identifier
    ;

determinism
    : DETERMINISTIC
    | NOT DETERMINISTIC;

nullCallClause
    : RETURNS NULL ON NULL INPUT
    | CALLED ON NULL INPUT
    ;

externalRoutineName
    : identifier
    ;

queryNoWith:
      queryTerm
      (ORDER BY sortItem (COMMA sortItem)*)?
      (OFFSET offset=INTEGER_VALUE (ROW | ROWS)?)?
      ((LIMIT limit=INTEGER_VALUE | (FETCH FIRST fetchFirstNRows=INTEGER_VALUE ROWS ONLY))?)?
    ;

queryTerm
    : queryPrimary                                                             #queryTermDefault
    | left=queryTerm operator=INTERSECT setQuantifier? right=queryTerm         #setOperation
    | left=queryTerm operator=(UNION | EXCEPT) setQuantifier? right=queryTerm  #setOperation
    ;

queryPrimary
    : querySpecification                    #queryPrimaryDefault
    | TABLE qualifiedName                   #table
    | VALUES expression (COMMA expression)* #inlineTable
    | L_PAREN queryNoWith  R_PAREN          #subquery
    ;

sortItem
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (COMMA selectItem)*
      (FROM relation) //FROM is required in metalus sql, also legacy joins not supported
      where?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

where
    : WHERE booleanExpression
    ;

groupBy
    : setQuantifier? groupingElement (COMMA groupingElement)*
    ;

groupingElement
    : groupingSet                                            #singleGroupingSet
    | ROLLUP L_PAREN (expression (COMMA expression)*)? R_PAREN         #rollup
    | CUBE L_PAREN (expression (COMMA expression)*)? R_PAREN           #cube
    | GROUPING SETS L_PAREN groupingSet (COMMA groupingSet)* R_PAREN   #multipleGroupingSets
    ;

groupingSet
    : L_PAREN (expression (COMMA expression)*)? R_PAREN
    | expression
    ;

namedQuery
    : name=identifier (columnAliases)? AS L_PAREN query R_PAREN
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?  #selectSingle
    | qualifiedName DOT ASTERISK    #selectAll
    | ASTERISK                      #selectAll
    ;

relation
    : left=relation
      ( CROSS JOIN right=sampledRelation
      | joinType JOIN rightRelation=relation joinCriteria
      | NATURAL joinType JOIN right=sampledRelation
      )                                           #joinRelation
    | sampledRelation                             #relationDefault
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    | LEFT? SEMI
    | LEFT? ANTI
    ;

joinCriteria
    : ON booleanExpression
    | USING L_PAREN identifier (COMMA identifier)* R_PAREN
    ;

sampledRelation
    : aliasedRelation (
        TABLESAMPLE sampleType L_PAREN percentage=expression R_PAREN
      )?
    ;

sampleType
    : BERNOULLI
    | SYSTEM
    ;

aliasedRelation
    : relationPrimary (AS? identifier columnAliases?)?
    ;

columnAliases
    : L_PAREN identifier (COMMA identifier)* R_PAREN
    ;

relationPrimary
    : dataReference                                                   #dataReferenceRelation
//    | qualifiedName                                                   #tableName
    | L_PAREN query R_PAREN                                                   #subqueryRelation
    | UNNEST L_PAREN expression (COMMA expression)* R_PAREN (WITH ORDINALITY)?  #unnest
    | LATERAL L_PAREN query R_PAREN                                           #lateral
    | L_PAREN relation R_PAREN                                                #parenthesizedRelation
    ;

dataReference
    : mapping
    | step
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?             #predicated
    | NOT booleanExpression                                        #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

setExpression
    : identifier EQ expression
    ;

// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression                            #comparison
    | comparisonOperator comparisonQuantifier L_PAREN query R_PAREN               #quantifiedComparison
    | NOT? BETWEEN lower=valueExpression AND upper=valueExpression        #between
    | NOT? IN L_PAREN expression (COMMA expression)* R_PAREN                        #inList
    | NOT? IN L_PAREN query R_PAREN                                               #inSubquery
    | NOT? LIKE pattern=valueExpression (ESCAPE escape=valueExpression)?  #like
    | IS NOT? NULL                                                        #nullPredicate
    | IS NOT? DISTINCT FROM right=valueExpression                         #distinctFrom
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | valueExpression AT timeZoneSpecifier                                              #atTimeZone
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression CONCAT right=valueExpression                                 #concatenation
    ;

primaryExpression
    : NULL                                                                                #nullLiteral
    | interval                                                                            #intervalLiteral
    | identifier string                                                                   #typeConstructor
    | DOUBLE_PRECISION string                                                             #typeConstructor
    | number                                                                              #numericLiteral
    | booleanValue                                                                        #booleanLiteral
    | string                                                                              #stringLiteral
    | BINARY_LITERAL                                                                      #binaryLiteral
//    | '?'                                                                                 #parameter
    | POSITION L_PAREN valueExpression IN valueExpression R_PAREN                                 #position
    | L_PAREN expression (COMMA expression)+ R_PAREN                                                #rowConstructor
    | ROW L_PAREN expression (COMMA expression)* R_PAREN                                            #rowConstructor
    | qualifiedName L_PAREN ASTERISK R_PAREN filter? over?                                        #functionCall
    | qualifiedName L_PAREN (setQuantifier? expression (COMMA expression)*)?
        (ORDER BY sortItem (COMMA sortItem)*)? R_PAREN filter? (nullTreatment? over)?           #functionCall
//    | identifier '->' expression                                                          #lambda
//    | L_PAREN (identifier (COMMA identifier)*)? R_PAREN '->' expression                             #lambda
    | L_PAREN query R_PAREN                                                                       #subqueryExpression
    // This is an extension to ANSI SQL, which considers EXISTS to be a <boolean expression>
    | EXISTS L_PAREN query R_PAREN                                                                #exists
    | CASE valueExpression whenClause+ (ELSE elseExpression=expression)? END              #simpleCase
    | CASE whenClause+ (ELSE elseExpression=expression)? END                              #searchedCase
    | CAST L_PAREN expression AS type R_PAREN                                                     #cast
    | TRY_CAST L_PAREN expression AS type R_PAREN                                                 #cast
    | ARRAY L_BRACKET (expression (COMMA expression)*)? R_BRACKET                                       #arrayConstructor
    | value=primaryExpression L_BRACKET index=valueExpression R_BRACKET                               #subscript
    | identifier                                                                          #columnReference
    | base=primaryExpression DOT fieldName=identifier                                     #dereference
    | name=CURRENT_DATE                                                                   #specialDateTimeFunction
    | name=CURRENT_TIME (L_PAREN precision=INTEGER_VALUE R_PAREN)?                                #specialDateTimeFunction
    | name=CURRENT_TIMESTAMP (L_PAREN precision=INTEGER_VALUE R_PAREN)?                           #specialDateTimeFunction
    | name=LOCALTIME (L_PAREN precision=INTEGER_VALUE R_PAREN)?                                   #specialDateTimeFunction
    | name=LOCALTIMESTAMP (L_PAREN precision=INTEGER_VALUE R_PAREN)?                              #specialDateTimeFunction
    | name=CURRENT_USER                                                                   #currentUser
    | SUBSTRING L_PAREN valueExpression FROM valueExpression (FOR valueExpression)? R_PAREN       #substring
    | NORMALIZE L_PAREN valueExpression (COMMA normalForm)? R_PAREN                                 #normalize
    | EXTRACT L_PAREN identifier FROM valueExpression R_PAREN                                     #extract
    | L_PAREN expression R_PAREN                                                                  #parenthesizedExpression
    | GROUPING L_PAREN (qualifiedName (COMMA qualifiedName)*)? R_PAREN                              #groupingOperation
    ;

nullTreatment
    : IGNORE NULLS
    | RESPECT NULLS
    ;

timeZoneSpecifier
    : TIME ZONE interval  #timeZoneInterval
    | TIME ZONE string    #timeZoneString
    ;

comparisonQuantifier
    : ALL | SOME | ANY
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? string from=intervalField (TO to=intervalField)?
    ;

intervalField
    : YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

normalForm
    : NFD | NFC | NFKD | NFKC
    ;

types
    : L_PAREN (type (COMMA type)*)? R_PAREN
    ;

type
    : type ARRAY
    | ARRAY LT type GT
    | MAP LT type COMMA type GT
    | ROW L_PAREN identifier type (COMMA identifier type)* R_PAREN
    | baseType (L_PAREN typeParameter (COMMA typeParameter)* R_PAREN)?
    | INTERVAL from=intervalField TO to=intervalField
    ;

typeParameter
    : INTEGER_VALUE | type
    ;

baseType
    : TIME_WITH_TIME_ZONE
    | TIMESTAMP_WITH_TIME_ZONE
    | DOUBLE_PRECISION
    | qualifiedName
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

filter
    : FILTER L_PAREN WHERE booleanExpression R_PAREN
    ;

over
    : OVER L_PAREN
        (PARTITION BY partition+=expression (COMMA partition+=expression)*)?
        (ORDER BY sortItem (COMMA sortItem)*)?
        windowFrame?
      R_PAREN
    ;

windowFrame
    : frameType=RANGE start=frameBound
    | frameType=ROWS start=frameBound
    | frameType=RANGE BETWEEN start=frameBound AND end=frameBound
    | frameType=ROWS BETWEEN start=frameBound AND end=frameBound
    ;

frameBound
    : UNBOUNDED boundType=PRECEDING                 #unboundedFrame
    | UNBOUNDED boundType=FOLLOWING                 #unboundedFrame
    | CURRENT ROW                                   #currentRowBound
    | expression boundType=(PRECEDING | FOLLOWING)  #boundedFrame // expression should be unsignedLiteral
    ;


explainOption
    : FORMAT value=(TEXT | GRAPHVIZ | JSON)                 #explainFormat
    | TYPE value=(LOGICAL | DISTRIBUTED | VALIDATE | IO)    #explainType
    ;

transactionMode
    : ISOLATION LEVEL levelOfIsolation    #isolationLevel
    | READ accessMode=(ONLY | WRITE)      #transactionAccessMode
    ;

levelOfIsolation
    : READ UNCOMMITTED                    #readUncommitted
    | READ COMMITTED                      #readCommitted
    | REPEATABLE READ                     #repeatableRead
    | SERIALIZABLE                        #serializable
    ;

callArgument
    : expression                    #positionalArgument
    | identifier ARG expression    #namedArgument
    ;

privilege
    : SELECT | DELETE | INSERT | identifier
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

step
    : stepName=IDENTIFIER L_PAREN (stepParam (COMMA stepParam)*)* R_PAREN
    ;

stepParam
    : (stepParamName=string ARG)? stepValue
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

nonReserved
    // IMPORTANT: this rule must only contain tokens. Nested rules are not supported. See SqlParser.exitNonReserved
    : ADD | ADMIN | ALL | ANALYZE | ANY | ARRAY | ASC | AT
    | BERNOULLI
    | CALL | CALLED | CASCADE | CATALOGS | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT | CURRENT_ROLE
    | DATA | DATE | DAY | DEFINER | DESC | DETERMINISTIC | DISTRIBUTED
    | EXCLUDING | EXPLAIN | EXTERNAL
    | FETCH | FILTER | FIRST | FOLLOWING | FORMAT | FUNCTION | FUNCTIONS
    | GRANT | GRANTED | GRANTS | GRAPHVIZ
    | HOUR
    | IF | IGNORE | INCLUDING | INPUT | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | LANGUAGE | LAST | LATERAL | LEVEL | LIMIT | LOGICAL
    | MAP | MATERIALIZED | MINUTE | MONTH
    | NAME | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | OFFSET | ONLY | OPTION | ORDINALITY | OUTPUT | OVER
    | PARTITION | PARTITIONS | POSITION | PRECEDING | PRIVILEGES | PROPERTIES
    | RANGE | READ | REFRESH | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNS | REVOKE | ROLE | ROLES | ROLLBACK | ROW | ROWS
    | SCHEMA | SCHEMAS | SECOND | SECURITY | SERIALIZABLE | SESSION | SET | SETS | SQL
    | SHOW | SOME | START | STATS | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEMPORARY | TEXT | TIME | TIMESTAMP | TO | TRANSACTION | TRUNCATE | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | USE | USER
    | VALIDATE | VERBOSE | VIEW
    | WORK | WRITE
    | YEAR
    | ZONE
    ;

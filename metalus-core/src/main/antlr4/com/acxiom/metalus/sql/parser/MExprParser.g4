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
 */
parser grammar MExprParser;

options { tokenVocab = MSqlLexer; caseInsensitive = true;}


singleStepExpression
    : stepExpression EOF
    ;

stepExpression
    : stepValueExpression                                                                        #stepValueExpr
    | NOT stepExpression                                                                         #booleanNot
    | left=stepExpression operator=(ASTERISK|SLASH|PERCENT) right=stepExpression                 #arithmetic
    | left=stepExpression operator=(PLUS|MINUS) right=stepExpression                             #arithmetic
    | left=stepExpression operator=(EQ|NEQ|LT|LTE|GT|GTE) right=stepExpression                   #booleanExpr
    | left=stepExpression operator=(AND|OR) right=stepExpression                                 #booleanExpr
    | IF L_PAREN ifExpr=stepExpression R_PAREN then=stepExpression ELSE elseExpr=stepExpression  #ifStatement
    | L_PAREN stepExpression R_PAREN                                                             #subExpr
    | left=stepExpression operator=CONCAT right=stepExpression                                   #stepConcat
    ;

stepValueExpression
    : stepValue                                               #stepVal
    | stepValueExpression DOT name=(EXISTS|FILTER|FIND|MAP|REDUCE) L_PAREN label=identifier ARG function=stepExpression R_PAREN  #lambda
    | stepValueExpression DOT name=(TO_ARRAY|TO_LIST|TO_MAP)  #toCollection
    | stepValueExpression L_BRACKET stepExpression R_BRACKET  #collectionAccess
    | left=stepValueExpression DOT right=stepValueExpression  #dereference
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | QUOTED_IDENTIFIER      #quotedIdentifier
    | nonReserved            #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    ;

mapping
    : symbol=(STEP_RETURN|SECONDARY_RETURN|GLOBAL|PARAMETER|R_PARAMETER|PERCENT|AMPERSAND) key=stepIdentifier
    ;

stepIdentifier
    : identifier (DOT identifier)*
    ;

stepValue
    : mapping                                              #pipelineMapping
    | stepLiteral                                          #literalVal
    | L_BRACKET (stepExpression (COMMA stepExpression)*)? R_BRACKET  #listValue
    | L_CURLY (mapParam (COMMA mapParam)*)? R_CURLY        #mapValue
    | SOME L_PAREN stepExpression R_PAREN                  #someValue
    | NONE                                                 #noneValue
    | object                                               #newObject
    | stepIdentifier                                       #variableAccess
    | reservedRef                                          #variableAccess
    ;

mapParam
    : sqlString COLON stepExpression
    ;

stepLiteral
    : sqlString     #stringLit
    | number        #numericLit
    | booleanValue  #booleanLit
    ;

sqlString
    : STRING                            #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?  #unicodeStringLiteral
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | DOUBLE_VALUE   #doubleLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

booleanValue
    : TRUE | FALSE
    ;

reservedRef
    : STEP
    | VALUE
    ;

object
    : stepIdentifier L_PAREN (stepExpression (COMMA stepExpression)*)? R_PAREN
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

nonReserved
    : ADD | ADMIN | ALL | ALTER | ANALYZE | AND | ANTI | ANY | ARRAY | ASC | AT
    | BERNOULLI | BETWEEN | BY
    | CALL | CALLED | CASCADE | CATALOGS | COLUMN | COLUMNS | COMMENT | COMMIT | COMMITTED | CURRENT | CURRENT_ROLE
    | DATA | DATE | DAY | DEFINER | DESC | DETERMINISTIC | DISTRIBUTED
    | EXCLUDING | EXECUTE | EXISTS | EXPLAIN | EXTERNAL
    | FETCH | FILTER | FIND | FIRST | FOLLOWING | FORMAT | FUNCTION | FUNCTIONS
    | GRANT | GRANTED | GRANTS | GRAPHVIZ
    | HOUR
    | IF | IGNORE | INCLUDING | INPUT | INTERVAL | INVOKER | IO | ISOLATION
    | JSON
    | LANGUAGE | LAST | LATERAL | LEFT | LEVEL | LIMIT | LOGICAL
    | MAP | MATERIALIZED | MINUTE | MONTH
    | NAME | NFC | NFD | NFKC | NFKD | NO | NONE | NULLIF | NULLS
    | OFFSET | ONLY | OPTION | ORDINALITY | OUTPUT | OVER
    | PARTITION | PARTITIONS | POSITION | PRECEDING | PRIVILEGES | PROPERTIES
    | RANGE | READ | REDUCE | REFRESH | RENAME | REPEATABLE | REPLACE | RESET | RESPECT | RESTRICT | RETURN | RETURNS
    | REVOKE | RIGHT | ROLE | ROLES | ROLLBACK | ROW | ROWS
    | SCHEMA | SCHEMAS | SECOND | SECURITY | SERIALIZABLE | SESSION | SET | SETS | SQL
    | SHOW | SOME | START | STATS | SUBSTRING | SYSTEM
    | TABLES | TABLESAMPLE | TEMPORARY | TEXT | TIME | TIMESTAMP | TO | TRANSACTION | TRUNCATE | TRY_CAST | TYPE
    | UNBOUNDED | UNCOMMITTED | USE | USER
    | VALIDATE | VERBOSE | VIEW
    | WORK | WRITE
    | YEAR
    | ZONE
    ;

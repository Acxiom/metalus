
parser grammar MExprParser;

options { tokenVocab = MSqlLexer; caseInsensitive = true;}


singleStepExpression
    : stepExpression EOF
    ;

//stepExpression
//    : primaryStepExpression (CONCAT primaryStepExpression)*
//    ;

stepExpression
    : left=stepExpression operator=CONCAT right=stepExpression   #stepConcat
    | left=stepExpression operator=PLUS right=stepExpression     #stringConcat
    | left=stepExpression operator=(AND|OR|EQ|NEQ) right=stepExpression #booleanExpr
    | IF L_PAREN ifExpr=stepExpression R_PAREN then=stepExpression ELSE elseExpr=stepExpression #ifStatement
    | NOT stepExpression                       #booleanNot
    | L_PAREN stepExpression R_PAREN           #subExpr
    | stepValue                                #literalExpr
    ;


mapping
    : symbol=(STEP_RETURN|SECONDARY_RETURN|GLOBAL|PARAMETER|R_PARAMETER|PERCENT|PIPELINE) key=stepIdentifier
    ;

stepIdentifier
    : IDENTIFIER (DOT IDENTIFIER)*
    ;

stepValue
    : mapping     #pipelineMapping
    | stepLiteral #literalVal
    | L_BRACKET (stepValue (COMMA stepValue)*)? R_BRACKET #listValue
    | L_CURLY (mapParam (COMMA mapParam)*)? R_CURLY #mapValue
    | reservedRef #reservedVal
    ;

mapParam
    : string COLON stepValue
    ;

stepLiteral
    : string       #stringLit
    | number       #numericLit
    | booleanValue #booleanLit
    ;

string
    : STRING                                #basicStringLiteral
    | UNICODE_STRING (UESCAPE STRING)?      #unicodeStringLiteral
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

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

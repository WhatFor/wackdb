While this grammar is not parsed by ANTLR, it uses the ANTLR grammar format.

```ANTLR
grammar Wack;

// -------------------------
// Program
// -------------------------
program
    : query* EOF
    ;

// -------------------------
// Query Statement
// -------------------------

query
    : (simpleStatement SEMICOLON_SYMBOL?)?
    ;

simpleStatement
    :
    selectStatement
    | insertStatement
    | updateStatement
    | deleteStatement
    ;

// -------------------------
// Query Types
// -------------------------

// Very basic. Doesn't support sub-queries, 'INTO', 'DISTINCT', 'UNION', 'ALL', 'TOP', expressions in SelectItem, column aliases.
selectStatement
    : selectExpressionBody orderClause? limitClause?
    ;

// TODO:
//   Supports optional fromClause, assuming `SELECT 1` is valid, though expressions not yet  supported.
selectExpressionBody
    : SELECT_SYMBOL selectItemList fromClause? whereClause? groupByClause?
    ;

selectItemList
    : selectItem (COMMA_SYMBOL selectItem)*
    ;

selectItem
    : identifier
    ;

fromClause
    : FROM_SYMBOL tableReference
    ;

whereClause
    :   WHERE_SYMBOL expr
    ;

groupByClause
    : GROUP_SYMBOL BY_SYMBOL orderList
    ;

orderClause
    : ORDER_SYMBOL BY_SYMBOL orderList
    ;

orderList
    : orderExpression (COMMA_SYMBOL orderExpression)?
    ;

orderExpression
    : expr direction?
    ;

direction
    : ASC_SYMBOL
    | DESC_SYMBOL
    ;

limitClause
    : LIMIT_SYMBOL limitOptions
    ;

limitOptions
    : NUMBER
    ;

// TODO: Ignoring TOP, WITH, CTEs, OUTPUT, etc. Just raw dog INSERT, baby.
insertStatement
    : INSERT_SYMBOL INTO_SYMBOL tableReference (insertColumnList) VALUES_SYMBOL insertValueListList
    ;
    
insertColumnList
    : insertColumn (COMMA_SYMBOL insertColumn)*
    ;

insertColumn
    : identifier
    ;
    
insertValueListList
    : insertValueList (COMMA_SYMBOL insertValueList)*
    ;
    
insertValueList
    : ( insertValue (COMMA_SYMBOL insertValue)* )
    ;

insertValue
    : expr 
    ;

// TODO
updateStatement:
deleteStatement:

// -------------------------
// Generic Stuff
// -------------------------

// TODO: blimey expressions are complex
expr
    :
    ;

tableReference
    : dotIdentifier
    ;

identifier
    ; identifier dotIdentifier?
    ;

dotIdentifier
    : DOT_SYMBOL identifier
    ;
```

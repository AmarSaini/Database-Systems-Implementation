 # Appending onto what SQL has
 SQL ::=
 CREATE TABLE TableName ( AttsExpression ) |
 LOAD DATA TableName FROM TextFile |
 CREATE INDEX IndexName FROM TableName ON AttributeName

 AttsExpression ::= AttributeName TypeName | AttsExpression ',' AttributeName TypeName
 AttributeName ::= YY_NAME
 TypeName ::= YY_NAME
 TableName ::= YY_NAME
 TextFile ::= YY_NAME
 IndexName ::= YY_NAME



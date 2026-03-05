from enum import Enum


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"

class AggregationFunction(Enum):
    NONE = ""
    COUNT = "COUNT"
    DISTINCT = "DISTINCT"
    COUNT_DISTINCT = "COUNT(DISTINCT)"
    SUM = "SUM"
    COUNT_ALL = "COUNT(*)"
    AVERAGE = "AVG"
    MINIMUM = "MIN"
    MAXIMUM = "MAX"

class QueryOperator(Enum):
    # Common operators
    EQUALS = "="
    NOT_EQUALS = "!="
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"

    # String-specific operators
    CONTAINS = "CONTAINS"
    NOT_CONTAINS = "NOT CONTAIN"
    STARTS_WITH = "STARTS WITH"
    ENDS_WITH = "ENDS WITH"
    MATCHES = "MATCHES"
    NOT_MATCHES = "NOT MATCHES"


    # Number and Date specific operators
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN_OR_EQUAL = "<="
    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT BETWEEN"
    IN = "IN"
    NOT_IN = "NOT IN"
    LIST_CONTAINS = 'LIST CONTAINS'
    LIST_NOT_CONTAINS = 'LIST NOT CONTAINS'


class SelectType(Enum):
    Normal = "NORMAL"
    aggregate = "AGGREGATE"

class HavingConditionType(Enum):
  FIELD = 'Field'
  COUNT_ALL = 'Count(*)'
  COUNT_COLUMN = 'Count(Column)'


class DateUnit(Enum):
  Day = 'DAY'
  Month = 'MONTH'
  Year = 'YEAR'


class ColumnType(Enum):
  String = 'string'
  Number = 'number'
  Date = 'Date'
  Boolean = 'boolean'
  Datetime = 'Datetime'
  List = 'list'
  Set = 'set'
  MultiSet = 'multiset'


class ComparisonType(Enum):
  Value = 'VALUE',   # Compare with a fixed value
  Column = 'COLUMN'  # Compare with another column
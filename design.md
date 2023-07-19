# 存储结构
- Data Block
- Index Block
- Block Info

# 详细结构

## Data Block
Data Block由多个KeyValue构成
### KeyValue
- KeyLength
- RowKeyLength
- RowKey
- ColumnNameLength
- ColumnName
- TimeStamp
- ValueType
- ValueLength
- Value

## Index Block
Index Block由多个Index Item组成
### Index Item
- Offset
- DataSize
- Key Length
- Key Value

## Block Info
- Index Block Offset
- Index Block Size
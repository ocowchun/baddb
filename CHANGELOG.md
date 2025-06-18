## 0.2.5 / 2025-06-18
* Improve `TransactWriteItems` error response 

## 0.2.4 / 2025-06-17
* Fix item not found logic, return nil instead of empty map
* Improve `GetItem` validation
  * Check if the key attributes are provided
  * Check if the key attributes match the table's key schema

## 0.2.3 / 2025-06-16
* Fix unsupported `<>` operator in `filterExpression` for `Scan`/`Query` operation.
* Fix incorrect JSON key for BOOL type
* Improve error handling
  * Check attribute value has a valid format 
  * Check attribute value type match key type
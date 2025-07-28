## 0.2.9 / 2025-07-28
* validate table and index names
* Fix `UpdateTable`, update inner storage `ProvisionedThroughput` setting.
* 
## 0.2.8 / 2025-07-28
* validate table and index names 
* Fix `UpdateTable`, update inner storage `ProvisionedThroughput` setting.

## 0.2.7 / 2025-07-23
* Support `UpdateTable` 

## 0.2.6 / 2025-06-24
* Fix `attribute_type` condition
* Add more test cases

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
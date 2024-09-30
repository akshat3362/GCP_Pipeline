CREATE OR REPLACE PROCEDURE `keen-dolphin-436707-b9.EDM_CONFIRMED_PRD.SP_LOAD_ORDERS` ()
BEGIN
  DECLARE ERROR_MESSAGE STRING; 

  BEGIN

  INSERT INTO keen-dolphin-436707-b9.EDM_CONFIRMED_PRD.Orders_BIM
  select 
  CAST(JSON_VALUE(json_data,'$.order_id') AS INT64), 
  CAST(JSON_VALUE(json_data,'$.customer_id') AS INT64),
  CAST(JSON_VALUE(json_data,'$.order_date') AS DATE),
  JSON_VALUE(json_data,'$.shipping_address.city'),
  JSON_VALUE(item,'$.item_id'),
  CAST(JSON_VALUE(item,'$.quantity') AS INT64),
  CAST(JSON_VALUE(item,'$.price') AS FLOAT64)
  from keen-dolphin-436707-b9.EDM_CONFIRMED_PRD.Orders,
  UNNEST(JSON_EXTRACT_ARRAY(json_data,'$.items')) AS item;

  EXCEPTION WHEN ERROR THEN
    SET ERROR_MESSAGE = "Error inserting data";

    INSERT INTO `keen-dolphin-436707-b9.EDM_CONFIRMED_PRD.Errors` 
    VALUES(ERROR_MESSAGE , CURRENT_TIMESTAMP());

  END;
END;

CALL `keen-dolphin-436707-b9.EDM_CONFIRMED_PRD.SP_LOAD_ORDERS`()
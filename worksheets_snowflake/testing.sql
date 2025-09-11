


SELECT 

original_id,
description__company_information,
description__needs,
description__requirements,
employer__email,
driving_license,
removed_date,



* FROM job_ads;

SELECT

occupation_group__label,
occupation_field__label

FROM job_ads;

desc table job_ads;

select DISTINCT OCCUPATION_FIELD__LABEL

 from JOB_ADS;




-- Network policies, testing

SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;

DESC USER ERIKUNEVIK;  -- kolla kolumnen NETWORK_POLICY
-- eller:
SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN USER ERIKUNEVIK;

SELECT * 
FROM TABLE(INFORMATION_SCHEMA.LOGIN_HISTORY())
WHERE USER_NAME = 'ERIKUNEVIK'
ORDER BY EVENT_TIMESTAMP DESC
LIMIT 50;

ALTER USER ERIKUNEVIK UNSET NETWORK_POLICY;

USE ROLE ACCOUNTADMIN;

ALTER ACCOUNT UNSET NETWORK_POLICY;

ALTER USER ERIKUNEVIK SET NETWORK_POLICY = OPEN_ALL;

--

USE ROLE ACCOUNTADMIN;         -- krävs för CREATE NETWORK POLICY

-- se vad som finns
SHOW NETWORK POLICIES;

-- skapa en policy som släpper alla IPv4 och IPv6 (osäkert – endast för labb!)
CREATE NETWORK POLICY OPEN_ALL
  ALLOWED_IP_LIST = ('0.0.0.0/0', '::/0');

-- sätt den på användaren (eller på kontot)
ALTER USER ERIKUNEVIK SET NETWORK_POLICY = OPEN_ALL;
-- alternativt: ALTER ACCOUNT SET NETWORK_POLICY = OPEN_ALL;

--

USE ROLE ACCOUNTADMIN;

CREATE NETWORK POLICY OPEN_ALL
  ALLOWED_IP_LIST = ('0.0.0.0/0');

ALTER USER ERIKUNEVIK SET NETWORK_POLICY = OPEN_ALL;
-- eller på kontonivå
-- ALTER ACCOUNT SET NETWORK_POLICY = OPEN_ALL;










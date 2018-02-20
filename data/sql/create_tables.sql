-- Table: claim

-- DROP TABLE claim;

CREATE TABLE claim
(
  ronumber character varying(20),
  productfamily character varying(250),
  countrycode character(2),
  submissiondate date,
  expectedresponsedate date,
  claimnumber character varying(20) NOT NULL,
  productcodeid character(13),
  indicationid character(13),
  claimtype character varying(10),
  factoryapi character varying(4),
  factorybulk character varying(4),
  factorypackage character varying(4),
  drugprice character varying(20),
  reimbursementprct smallint,
  CONSTRAINT claim_pkey PRIMARY KEY (claimnumber)
)

-- Table: drugproduct

-- DROP TABLE drugproduct;

CREATE TABLE drugproduct
(
  drugproductid character(13) NOT NULL,
  recordstatus character(1),
  drugproductname character varying(250),
  ronumber character varying(20),
  drugproductfamilyid integer,
  CONSTRAINT drugproduct_pkey PRIMARY KEY (drugproductid)
)

-- Table: drugproductfamily

-- DROP TABLE drugproductfamily;

CREATE TABLE drugproductfamily
(
  drugproductfamilyid serial NOT NULL,
  drugproductfamilyname character varying(250),
  CONSTRAINT drugproductfamily_pkey PRIMARY KEY (drugproductfamilyid)
)

-- Table: indication

-- DROP TABLE indication;

CREATE TABLE indication
(
  indicationid character(13) NOT NULL,
  recordstatus character(1),
  indicationdescription character varying(500),
  CONSTRAINT indication_pkey PRIMARY KEY (indicationid)
)



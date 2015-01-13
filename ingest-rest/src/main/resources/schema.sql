create table users (
   username varchar(256),
   password varchar(256),
   enabled boolean
);

create table authorities (
  username varchar(256),
  authority varchar(256)
);

CREATE TABLE bag (
  id bigint generated by DEFAULT as IDENTITY (start WITH 1),
  name VARCHAR(255),
  depositor VARCHAR(255),
  location VARCHAR(255),
  token_location VARCHAR(255),
  token_digest VARCHAR(255),
  tag_manifest_digest VARCHAR(255),
  status VARCHAR(255),
  fixity_algorithm VARCHAR(255),
  size bigint not null,
  PRIMARY KEY (id)
);

create table node (
  id bigint generated by DEFAULT as IDENTITY (start WITH 1),
  enabled boolean,
  password VARCHAR(255),
  username VARCHAR(255),
  PRIMARY KEY (id)
);

CREATE TABLE replication (
  replicationid bigint generated by DEFAULT as IDENTITY (start WITH 1),
  status VARCHAR (255),
  bag_link VARCHAR (255),
  token_link VARCHAR (255),
  protocol VARCHAR (255),
  received_tag_fixity VARCHAR (255),
  received_token_fixity VARCHAR (255),
  id bigint,
  node_id bigint,
  PRIMARY KEY (replicationid)
);

CREATE TABLE restoration (
  restoration_id bigint generated by DEFAULT as IDENTITY (start WITH 1),
  depositor VARCHAR (255),
  link VARCHAR (255),
  name VARCHAR (255),
  protocol VARCHAR (255),
  status VARCHAR (255),
  node_id bigint,
  PRIMARY KEY (restoration_id)
);

ALTER TABLE replication
  ADD CONSTRAINT FK_repl_bag FOREIGN KEY (id) REFERENCES bag;

ALTER TABLE replication
  ADD CONSTRAINT FK_repl_node FOREIGN KEY (node_id) REFERENCES node;

ALTER TABLE restoration
  ADD CONSTRAINT FK_rest_node FOREIGN KEY (node_id) REFERENCES node;

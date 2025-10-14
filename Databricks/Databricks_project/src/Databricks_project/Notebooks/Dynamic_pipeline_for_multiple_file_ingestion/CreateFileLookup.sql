CREATE TABLE IF NOT  EXISTS dev_catalog.bronze.file_name_metadata_table
(
  file string,
  type string
);



INSERT INTO dev_catalog.bronze.file_name_metadata_table
VALUES
('AdventureWorks_Territories', 'csv');


select * from dev_catalog.bronze.file_name_metadata_table;
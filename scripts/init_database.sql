/*
================================================
Create Database and Schemas
================================================
Purpose:
    This script creates a new database called 'data_warehouse'.
    It checks if the 'data_warehouse' database already exist; if yes, it drop the existing database and recreate.
    Additionally, the script creates three schemas within the database

WARNING:
    Running this script will drop the entire 'data_warehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


-- drop and create the 'data_warehouse'
DROP DATABASE IF EXISTS "data_warehouse";
CREATE DATABASE "data_warehouse";

-- create schema
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

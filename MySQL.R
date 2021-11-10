library(DBI)
library(odbc)
library(tidyverse)

con <- dbConnect(odbc(), dsn = "MySQL")

odbcListObjects(con)
# name    type
# 1 information_schema catalog
# 2              mysql catalog
# 3 performance_schema catalog
# 4             sakila catalog
# 5                sys catalog
# 6               test catalog
# 7              world catalog

setwd("C:\\Users\\User\\OneDrive\\Desktop\\MySQL")

getwd()

dbRemoveTable(con, "loan")

dbListTables(con)
# [1] "employee"   "peopleinfo"

df <- read_csv("CW1_credit_data_cleaned.csv")

dbWriteTable(con, "loan", df)

dbSendQuery(con,'DROP DATABASE IF EXISTS testDB')

odbcListObjects(con)

# dbSendQuery(con,'CREATE DATABASE testDB')
# 
# odbcListObjects(con)

library(explore)
dwh <- dwh_connect("MySQL")

data <- dwh_read_table(dwh, "loan")

data <- dwh_read_data(dwh, sql = "select * from loan")

# dwh_read_data(dwh, sql = "CREATE DATABASE testDB")

dwh_read_data(dwh, sql = "DROP DATABASE IF EXISTS testDB")

dwh_read_data(dwh, sql = "CREATE TABLE Persons(
PersonID int, LastName varchar(255), FirstName varchar(255),
Address varchar(255), City varchar(255))")

dwh_read_data(dwh, sql = "INSERT INTO Persons(
PersonID, LastName, FirstName, Address, City)
VALUES (1, 'Tom', 'Erichsen', 'Norway', 'Norway')")

dwh_read_data(dwh, sql = "DROP TABLE Persons")

dwh_fastload(data, "MySQL", "test.db")

dwh_read_data(dwh, sql = "DROP TABLE db")

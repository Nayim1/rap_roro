
# Script 1: SARH DATA PREP ----------------------------------------------------------
# raw data source: sarh0112 data to Dec 2017, from  publication, 
# 3 excel sheets row bound, saved as csv and put into sql as table "sarh0112_to_201712"
# each quarter the data in sql will need updating (and named something sensible that doens't need changing)

# this script sets the environment tools for all following scripts, reads in the data and 
# prepares it for use in tables 0101 to 0112
# I think 0113 and 0114 use other data as well/instead???

# # set up environment ------------------------------------------------------
# library(tidyverse) #broom, cli, crayon, dplyr, dbplyr, forcats, ggplot2, haven, hms, httr, jsonlite, lubridate, magrittr, modelr, purrr, readr, readxl, reprex, rlang, rstudioapi, rvest, stringr, tibble, tidyr, xml2 
# library(sqldf)
# library(purrr)
# library(utils)
# library(readODS)
# library(roxygen2)
# library(testthat)
# library(LStest) #Luke Shaw package specification of xltabr for DfT styles

# packages used -----------------------------------------------------------

.libPaths(c("C:/Program Files/R/R-3.4.3/library","P:/My Documents/R/Win-Library/3.4"))

library(dplyr)
library(readr)
library(stringr)
library(lubridate)
library(reshape2)
library(xltabr)
library(openxlsx)
library(devtools)
library(dbplyr)
library(RODBC)
library(tidyverse)
library(readxl)

# dplyr
# Warning messages:
#   1: In dir.create(tempPath, recursive = TRUE) :
#   cannot create dir 'G:\AFP', reason 'Permission denied'
#   2: In readLines(con) :
#   incomplete final line found on 'C:/Program Files/R/R-3.4.3/library/raw/rmarkdown/templates/reserveReview/template.yaml'


# create connection to SQL and fetch data-----------------------------------
# if you don't have permission to access this dataset then this will not work!
conn <- RODBC::odbcDriverConnect(
  'driver={SQL Server};server=TS01;database=IRHS_RORO_LIVE;trusted_connection=TRUE'
)


#raw data in SQL from 1 source
#tblRoRo covers 20040101 to 20180630 (and future)
#read these in, take what cols I want, bind_rows()
roro <- RODBC::sqlFetch(channel = conn
                        ,sqtable = "tblRoRo"
                        ,stringsAsFactors = FALSE
                        ,na.strings = ""
) %>% #select only cols required
  dplyr::select(
     -`EnteredDate`
) %>% #filter out NIR destination
  dplyr::filter(Year > 2003 & 
    DestinationCountry != "NIR"
) %>% 
  dplyr::mutate(Year_Quarter = paste(Year," Q",sep="",Quarter)
) %>%
  dplyr::mutate(Year_Quarter2 = if_else(condition = Quarter %in% 1,paste(Year," Q",sep="",Quarter),paste(" Q",sep="",Quarter),missing = NULL) 
) 
  

#truncate `Task Start` to char string format hh:mm:ss
arcc$`Task Start` <- stringr::str_sub(arcc$`Task Start`, start = 12, end = 19)


isar <- RODBC::sqlFetch(channel = conn
                        ,sqtable = "ISARdata"
                        ,stringsAsFactors = FALSE
                        ,na.strings = ""
)  %>% 
  dplyr::select(
    Count
    ,`Date`  
    ,`Base`
    ,`Dft Category`
    ,`New Tasking location`
    ,`Outcome`
    ,`Latitude (ARC GIS format)`
    ,`Longitude (Arc GIS format)`
    ,`Task Start`
    ,`DurationTotal Minutes`
    ,`Manual Northern Ireland input`
  ) %>% #rename
  dplyr::rename(
    `Type of tasking` = `Dft Category`
    ,`Tasking location` = `New Tasking location`
    ,`Tasking outcome` = `Outcome`
    ,Latitude = `Latitude (ARC GIS format)`
    ,Longitude = `Longitude (Arc GIS format)`
    ,`Duration total minutes` = `DurationTotal Minutes`
    ,Region = `Manual Northern Ireland input` 
  ) 
#tidy `Task Start` to regular format char string "hh:mm:ss"
#!!! 12 irregular entries which need sorting out first:
#11 entries which contain the string "01/01/1900"
#first detect the 11 entries with "01/01/1900" and sub for the final 9 chars, in the 3 cases that have less than 12 chars, these become ""
isar$`Task Start`[stringr::str_detect(
  string = isar$`Task Start`, pattern = "01/01/1900")] <- stringr::str_sub(
    string = isar$`Task Start`[stringr::str_detect(string = isar$`Task Start`, pattern = "01/01/1900")], start = 12, end = 19)
#sub the three "" for "00:00:00"
isar$`Task Start`[isar$`Task Start` == ""] <- "00:00:00"
#sub the single "7:05:00 AM" for "07:05:00"
isar$`Task Start`[stringr::str_detect(string = isar$`Task Start`, pattern = "AM$")] <- "07:05:00"


#glimpse(isar)
#glimpse(arcc)

#combine data sets 
sarh_raw <- isar %>% dplyr::bind_rows(arcc)

#rename and filter for count = 1 to get published figures
sarh <- sarh_raw %>% 
  dplyr::filter(Count == 1)

#glimpse(roro)

# Tidy data ---------------------------------------------------------------

#   -----------------------------------------------------------------------

# Consistency -------------------------------------------------------------
#check data types and column names
glimpse(sarh)

#check Base names consistent
frequencies::freq_vect(sarh$Base)
#make Base names consistent (NB white space in "Sumburgh " not stripped when read in from SQL table, but is stripped
#when read in from .csv by read_csv)
sarh <- sarh %>% 
  dplyr::mutate(Base = replace(Base
                               ,Base %in% c("Lee on Solent", "lee on solent")
                               ,"Lee On Solent")
                ,Base = replace(Base
                                ,Base == "Sumburgh "
                                ,"Sumburgh")
                )

#check type of tasking values consistent
frequencies::freq_vect(sarh$`Type of tasking`)
#make types of tasking values consistent
sarh <- sarh %>% 
  dplyr::mutate(`Type of tasking` = replace(`Type of tasking`
                                            ,`Type of tasking`=="Pre-arranged transfer"
                                            ,"Pre-arranged Transfer")
                ,`Type of tasking` = replace(`Type of tasking`
                                             ,`Type of tasking`=="Rescue/ Recovery"
                                             ,"Rescue/Recovery")
                ,`Type of tasking` = replace(`Type of tasking`
                                             ,`Type of tasking`%in% c("Search (only)", "Search (Only)", "Search only")
                                             ,"Search Only")
                )

#check tasking location values consistent
frequencies::freq_vect(sarh$`Tasking location`) #they are! (hooray)


#check tasking outcome values consistent

ts <- frequencies::freq_vect(sarh$`Tasking outcome`) #they are not (sigh)
#make tasking outcome values consistent
#read in lookup table (foruntately the ISAR cats and ARCC cats do not intersect)
lookup_tasking_outcome <- readr::read_csv(file = "data/lookup_tasking_outcome.csv")

#select appropriate cols from lookup and join to sarh
#NB ARCC categories is complete for all years, ISAR categories only for ISAR data (2015/17)
sarh <- sarh %>% 
  dplyr::left_join(select(lookup_tasking_outcome
                          ,`Data source`
                          ,`Tasking outcome`
                          ,`Published ISAR tasking outcome`
                          ,`Published ARCC tasking outcome`
                          ,`Published tasking outcome`)
                   ,by = "Tasking outcome"
                   )

#Region
#make regions consistent,should be 15 unique regions inc. NI
sarh$Region[sarh$Region == "SouthEast"] <- "South East"
sarh$Region[sarh$Region == "Northern Ireland "] <- "Northern Ireland"


# Extra variables -----------------------------------------------------------
#extra columns needed from lookups
lookup_region_country <- readr::read_csv(file = "data/lookup_region_country.csv")
sarh <- sarh %>%
  dplyr::left_join(lookup_region_country, by = "Region")

#calculate extra variables needed 
sarh <- sarh %>% 
  dplyr::mutate(
    `Day of week` = lubridate::wday(Date, label = TRUE, abbr = FALSE, week_start = 1) #day 1 = Mon
    ,Year = lubridate::year(Date)
    ,Month = lubridate::month(Date
                              ,label = TRUE
                              ,abbr = FALSE)
    ,Quarter_year = lubridate::quarter(Date
                                       ,with_year = TRUE)
    ,Quarter = lubridate::quarter(Date)
    ,`Duration rounded hours` = as.integer(round(`Duration total minutes`/ 60))
    ,`Task Start (time)` = lubridate::hms(sarh$`Task Start`)
    ,`Time of Day` = 
      dplyr::case_when(
        `Task Start (time)` < lubridate::hms("03:00:00") ~  "12am - 2:59am"
        ,`Task Start (time)` < lubridate::hms("06:00:00") ~ "3am - 5:59am"
        ,`Task Start (time)` < lubridate::hms("09:00:00") ~ "6am - 8:59am"
        ,`Task Start (time)` < lubridate::hms("12:00:00") ~ "9am - 11:59am"
        ,`Task Start (time)` < lubridate::hms("15:00:00") ~ "12pm - 2:59pm"
        ,`Task Start (time)` < lubridate::hms("18:00:00") ~ "3pm - 5:59pm"
        ,`Task Start (time)` < lubridate::hms("21:00:00") ~ "6pm - 8:59pm"
        ,`Task Start (time)` < lubridate::hms("23:59:59") ~ "9pm - 11:59pm"
      )

  )


# compare_time <- dplyr::select(sarh, `Task Start`, `Task Start (time)`, `Time of Day`)
# #functionalise setting the 0's to NAs
# #tables 01, 04 only - no point in functionalising
# #create table to use as lookup for Base closures
# base_closures <- tibble(Base = c("Caernarfon", "Lydd", "Newquay", "Prestwick", "Portland")
#                         ,Quarter_open_until = c(2015.2, 2015.2, 2015.4, 2015.4, NA)
#                         ,Quarter_closed_from = c(NA, NA, NA, NA, 2017.3)
# )


# #create table of quarterly total names
# #this uses calendar quarters, as does lubridate::quarter
# quarter_name_total <- tibble(Quarter = 1:4 
#                              ,Quarter_name_total = c("Jan to Mar total"
#                                                      ,"Apr to Jun total"
#                                                      ,"Jul to Sep total"
#                                                      ,"Oct to Dec total")
# )  



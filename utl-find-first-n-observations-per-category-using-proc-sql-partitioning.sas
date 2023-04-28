%let pgm=utl-find-first-n-observations-per-category-using-proc-sql-partitioning;

Find first n per category using proc sql partitioning

Select the first two females and first two males from sashelp.class using sql

 Four soltions
     1. SAS SQL Partitioning (has other uses category partition acts like a rownumber)
     2. WPS SQL Partitioning (has other uses category partition acts like a rownumber))
     3. SAS SQL
     4. WPS SQL
     5. WPS proc r SQL
     6. Python SQL

Renated to
StackOverflow
https://tinyurl.com/y76byevc
https://stackoverflow.com/questions/75014241/find-top-n-per-category-using-proc-sql

related
github
https://tinyurl.com/47ncxbvy
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning

As a side note you can use outobs wuth a sort to get the n largest values.

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

data class;
  set sashelp.class(keep=name sex);
run;quit;

/**************************************************************************************************************************/
/*                                                     |                                                                  */
/*  Table WORK.CLASS total obs=19 28APR2023:11:10:29   |  RULES SELECT                                                    */
/*                                                     |                                                                  */
/*  Obs    NAME       SEX                              |     Obs    NAME   ROW   SEX                                      */
/*                                                     |                                                                  */
/*    1    Alfred      M                               |       1    Alfred  1    M   ====> Select 1st male                */
/*    2    Alice       F                               |       2    Alice   1    F   ====> Select 1st female              */
/*    3    Barbara     F                               |       3    Barbara 2    F   ====> Select 2nd female              */
/*    4    Carol       F                               |                                                                  */
/*    5    Henry       M                               |       5    Henry   2    M   ----> Select 2nd male                */
/*    6    James       M                               |                                                                  */
/*    7    Jane        F                               |                                                                  */
/*    8    Janet       F                               |                                                                  */
/*    9    Jeffrey     M                               |                                                                  */
/*   10    John        M                               |                                                                  */
/*   11    Joyce       F                               |                                                                  */
/*   12    Judy        F                               |                                                                  */
/*   13    Louise      F                               |                                                                  */
/*   14    Mary        F                               |                                                                  */
/*   15    Philip      M                               |                                                                  */
/*   16    Robert      M                               |                                                                  */
/*   17    Ronald      M                               |                                                                  */
/*   18    Thomas      M                               |                                                                  */
/*   19    William     M                               |                                                                  */
/*                                                     |                                                                  */
/**************************************************************************************************************************/
/*                _   _ _   _                           _               _
 _ __   __ _ _ __| |_(_) |_(_) ___  _ __     ___  _   _| |_ _ __  _   _| |_
| `_ \ / _` | `__| __| | __| |/ _ \| `_ \   / _ \| | | | __| `_ \| | | | __|
| |_) | (_| | |  | |_| | |_| | (_) | | | | | (_) | |_| | |_| |_) | |_| | |_
| .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_|  \___/ \__,_|\__| .__/ \__,_|\__|
|_|                                                        |_|
*/

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from last table WORK.PARTITION total obs=4 28APR2023:11:22:16                                             */
/*                                                                                                                        */
/* Obs    PARTITION     NAME      SEX  NOTE THE ADDED PARTITION VARIABLE                                                  */
/*                                                                                                                        */
/*  1         1        Alice       F                                                                                      */
/*  2         2        Barbara     F                                                                                      */
/*  3         1        Alfred      M                                                                                      */
/*  4         2        Henry       M                                                                                      */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                _   _ _   _
 ___  __ _ ___   _ __   __ _ _ __| |_(_) |_(_) ___  _ __
/ __|/ _` / __| | `_ \ / _` | `__| __| | __| |/ _ \| `_ \
\__ \ (_| \__ \ | |_) | (_| | |  | |_| | |_| | (_) | | | |
|___/\__,_|___/ | .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_|
                |_|
*/

 proc sql;
   create table partition as
      select monotonic() as partition , name, sex from class where sex="M" and monotonic() < 3 union
      select monotonic() as partition , name, sex from class where sex="F" and monotonic() < 3
   order
      by sex
 ;quit;

/*                                     _   _ _   _
__      ___ __  ___   _ __   __ _ _ __| |_(_) |_(_) ___  _ __
\ \ /\ / / `_ \/ __| | `_ \ / _` | `__| __| | __| |/ _ \| `_ \
 \ V  V /| |_) \__ \ | |_) | (_| | |  | |_| | |_| | (_) | | | |
  \_/\_/ | .__/|___/ | .__/ \__,_|_|   \__|_|\__|_|\___/|_| |_|
         |_|         |_|
*/

%let _pth=%sysfunc(pathname(work));

%utl_submit_wps64('

options validvarname=any;

libname wrk "&_pth";

 proc sql;
   create table partition as
      select monotonic() as partition , name, sex from wrk.class where sex="M" and monotonic() < 3 union
      select monotonic() as partition , name, sex from wrk.class where sex="F" and monotonic() < 3
   order
      by sex
 ;quit;

proc print;
run;quit;

');

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/* Obs    partition     NAME      SEX                                                                                     */
/*                                                                                                                        */
/*  1         1        Alice       F                                                                                      */
/*  2         2        Barbara     F                                                                                      */
/*  3         1        Alfred      M                                                                                      */
/*  4         2        Henry       M                                                                                      */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                         _
 ___  __ _ ___   ___  __ _| |
/ __|/ _` / __| / __|/ _` | |
\__ \ (_| \__ \ \__ \ (_| | |
|___/\__,_|___/ |___/\__, |_|
                        |_|
*/

/*---- easy to multitask ---*/

proc sql;
    create table want as
      select name, sex from class where sex='M' and monotonic() < 3 union
      select name, sex from class where sex='F' and monotonic() < 3
    order
      by sex
;quit;

 /**************************************************************************************************************************/
 /*                                                                                                                        */
 /* Up to 40 obs from WANT total obs=4 28APR2023:12:00:51                                                                  */
 /* Obs     NAME      SEX                                                                                                  */
 /*                                                                                                                        */
 /*  1     Alice       F                                                                                                   */
 /*  2     Barbara     F                                                                                                   */
 /*  3     Alfred      M                                                                                                   */
 /*  4     Henry       M                                                                                                   */
 /*                                                                                                                        */
 /**************************************************************************************************************************/

/*                              _
__      ___ __  ___   ___  __ _| |
\ \ /\ / / `_ \/ __| / __|/ _` | |
 \ V  V /| |_) \__ \ \__ \ (_| | |
  \_/\_/ | .__/|___/ |___/\__, |_|
         |_|                 |_|
*/

%let _pth=%sysfunc(pathname(work));

%utl_submit_wps64('

libname wrk "&_pth";
proc sql;
    create table want as
      select name, sex from wrk.class where sex="M" and monotonic() < 3 union
      select name, sex from wrk.class where sex="F" and monotonic() < 3
    order
      by sex
;quit;

proc print;
run;quit;

');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs     NAME      SEX                                                                                                 */
/*                                                                                                                        */
/*   1     Alice       F                                                                                                  */
/*   2     Barbara     F                                                                                                  */
/*   3     Alfred      M                                                                                                  */
/*   4     Henry       M                                                                                                  */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*                                                            _
__      ___ __  ___   _ __  _ __ ___   ___   _ __   ___  __ _| |
\ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__| / __|/ _` | |
 \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |    \__ \ (_| | |
  \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|    |___/\__, |_|
         |_|         |_|                                   |_|
*/

/*---- newer version of sqllite has row_number? sqllite3? ----*/


%let _pth=%sysfunc(pathname(work));

%utl_submit_wps64("

libname wrk '&_pth';

proc r;
export data=wrk.class r=class;
submit;
  library(sqldf);
  males  <-sqldf('select name, sex from class where sex=\'M\' limit 2');
  females<-sqldf('select name, sex from class where sex=\'F\' limit 2');
  want   <-sqldf('select * from males union select * from females order by sex');
  want;
endsubmit;
import data=want_r r=want;
proc print data=wantx;
run;quit;
");

/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS System                                                                                                         */
/*                                                                                                                        */
/*      NAME SEX                                                                                                          */
/* 1   Alice   F                                                                                                          */
/* 2 Barbara   F                                                                                                          */
/* 3  Alfred   M                                                                                                          */
/* 4   Henry   M                                                                                                          */
/*                                                                                                                        */
/**************************************************************************************************************************/


/*           _   _                             _
 _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
| |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
| .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
|_|    |___/                                |_|
*/

libname  sd1 "d:/sd1";
data sd1.have;
  set sashelp.class;
run;quit;

%utlfkil(d:/xpt/res.xpt);

%utl_pybegin;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat")
males   = pdsql("""select name, sex from have where sex=\'M\' limit 2""")
females = pdsql("""select name, sex from have where sex=\'F\' limit 2""")
res     = pdsql("""select * from males union select * from females order by sex""")
print(res);
ds = xport.Dataset(res, name='res')
with open('d:/xpt/res.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend;

libname pyxpt xport "d:/xpt/res.xpt";

proc contents data=pyxpt._all_;
run;quit;

proc print data=pyxpt.res;
run;quit;

data res;
   set pyxpt.res;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* Up to 40 obs from RES total obs=4 28APR2023:16:03:32                                                                   */
/*                                                                                                                        */
/* Obs     NAME      SEX                                                                                                  */
/*                                                                                                                        */
/*  1     Alice       F                                                                                                   */
/*  2     Barbara     F                                                                                                   */
/*  3     Alfred      M                                                                                                   */
/*  4     Henry       M                                                                                                   */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/

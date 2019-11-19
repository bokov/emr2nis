#' ---
#' title: "Adapting Epic/Sunrise Data to Mimic HCUP NIS"
#' subtitle: "Single-file self-contained scri-port"
#' author: 
#'   - "Alex F. Bokov"
#'   - "Joel Michalek"
#' date: sys.Date()
#' ---
#' 
#+ init, message=FALSE,echo=FALSE
# init -----
inputdata <- './HSC20170563N.zip';
githost <- 'https://github.com/';
gitrepo <- 'bokov/emr2nis'
if(!require('devtools')) install.packages('devtools');
if(!require('trailR')) install_github('bokov/trailR');
if(!require('rio')) install_github('bokov/rio');
if(!require('tidbits')) install_github('bokov/tidbits');
instrequire(c('data.table','dplyr'));

.version <- trailR::gitstamp(prod=TRUE);
if(identical(.version,'TEST_OUTPUT_DO_NOT_USE')||length(.version)==0){
  .version <- c('master','TEST_OUTPUT_DO_NOT_USE');
};
if(tidbits:::git_(sprintf('rev-list --count origin/%s...HEAD',.version[1])
                  ,intern=T,VERBOSE=F)!=0){
  .version[2] <- 'TEST_OUTPUT_DO_NOT_USE'};
#' #### Reproducibility notice
#' 
#' This script relies on a private dataset that will be provided to 
#' authorized collaborators on HSC20170563N separately. No simulated version of
#' the data currently exists for testing. 
#' 
#' However, the code is decoupled from the data and is available for anybody
#' to review in the `r sprintf('[%2$s commit](%3$s%4$s/tree/%2$s) of the [%4$s](%3$s%4$s) repository **%1$s** branch',.version[1],.version[2],githost,gitrepo)`
#' 
#' The most current compiled version of this report can be viewed at 
#' https://rpubs.com/bokov/emr2nis_data_prep
#' 
#' ***
#' #### Read lookup tables
rc_disp <- fread('recode_dispuniform.csv');
rc_pay1 <- fread('recode_pay1.csv');
rc_race <- fread('recode_race.csv');

#' ***
#' #### Set required columns
requestedcols <- c('YEAR','DISPUNIFORM','PAY1','AGE','RACE','FEMALE'
                   ,'diabetes','liver','kidney','laproscopic_neph'
                   ,'robotic_neph', 'neph', 'h_neph','died', 'hhc', 'routine'
                   ,'transferother', 'transfershort', 'hispanic', 'newpay');
#' 
#' ***
#' #### Read main data file and deal with dates
#+ read_in_data, cache=TRUE
raw00 <- fread(unzip(inputdata),na.strings='',key=c('patient_num','start_date'))[
  ,start_date := as.Date(start_date)][,YEAR:=format(start_date,'%Y')];

#' 
dat00 <- raw00;
#' ***
#' #### Create `DISPUNIFORM` variable to mimic HCUP
#' 
#' **Caveat**: there is often more than one discharge code per patient-year,
#' and some guesswork was involved in matching them to the closest unified
#' HCUP code. Please see 
#' `r sprintf('[recode_dispuniform.csv](%1$s%2$s/blob/%3$s/recode_dispuniform.csv)',githost,gitrepo,.version[2])` 
#' and draw your own conclusions.
#' Note in particular that `6`, 
#' [Home Health Care](https://www.hcup-us.ahrq.gov/db/vars/dispuniform/nisnote.jsp)
#' does not have any equivalent codes in our local data under our current ETL.
#' Therefore the `hhc` analytic variable will always be `0`.
dat00[,DISPUNIFORM := submulti(v000_Sts__ptnts_cd,rc_disp,method='full')][
      ,DISPUNIFORM := ifelse(is.na(DISPUNIFORM),'.',DISPUNIFORM)];
#' 
#' ***
#' #### Create `PAY1` variable to mimic HCUP
#' 
#' **Caveat**: there is usually more than one payer code per patient-year,
#' and some guesswork was involved in matching them to the closest unified
#' HCUP code. Please see 
#' `r sprintf('[recode_dispuniform.csv](%1$s%2$s/blob/%3$s/recode_dispuniform.csv)',githost,gitrepo,.version[2])`
#' and draw your own conclusions.
dat00[,PAY1 := submulti(v001_Fncl_ptnts_cd,rc_pay1,method='full')];
#' 
#' ***
#' #### Create `AGE`, `FEMALE`, and `RACE` variables to mimic HCUP
dat00[,AGE:=round(min(age_at_visit_days)/365.25)
      ,by=list(patient_num,YEAR)][
        ,AGE:=ifelse(AGE<0,'.A',AGE)][
          ,FEMALE:=recode(sex_cd,m='0',f='1',.default='.')][
            ,RACE := submulti(race_cd,rc_race,method='full')];
#' Hispanic ethnicity is a separate variable in our EMR, but we overwrite the 
#' patient's actual race with `3` if they are of Hispanic ethnicity
#' 
dat00[v002_Ethnct__ptnts_cd=='DEM|ETHNICITY:Y'
      ,RACE := '3'];
#' 
#' ***
#' #### Create the obligate analytic variables.
#' 
#' These variables are: `diabetes`,`liver`,`kidney`,`laproscopic_neph`,
#' `robotic_neph`, `neph`, and `h_neph` should not be (re)calculated 
#' analytically within SAS when using this dataset because the underlying 
#' diagnosis codes are not in the same format or column structure as HCUP
#' 
#' **Caveat**: Do not trust `h_neph`-- in this version it should have been V4573 
#' but instead V457 was pulled which seems to not include V4573 though it's 
#' supposed to. Will try to fix this in next update of data pull.
#' 
#' **Caveat**: `neph` is under-reporting nephrectomies. The SAS code currently
#' only pulls partial nephrectomies (55.4). I made mine do the equivalent, but
#' in my opinion both should also pull full nephrectomies (55.5) (the commented
#' out line).
#' 
#' **Caveat**: `robotic_neph` is not necessarily a robotically assisted 
#' nephrectomy, both here and in the SAS code. The code 17.4 is for robotc
#' assisted procedures _in general_.
dat00[,`:=`(diabetes=1*v010_mlts__ptnts_tf
            ,liver=1*v009_intrhptc_ptnts_tf
            ,kidney=1*v008_Mlgnt_ptnts_tf
            ,laproscopic_neph=1*v004_Lprscp_ptnts_tf
            ,robotic_neph=1*v003_prcdrs_ptnts_tf
            ,neph=1*v005_nphrctm_ptnts_tf
            #,neph=1*(v005_nphrctm_ptnts_tf|v006_nphrctm_ptnts)
            ,h_neph=0)];
#' 
#' ***
#' #### Create other analytic variables
#' 
#' These variables are: `died`, `hhc`, `routine`, `transferother`, 
#' `transfershort`, `hispanic`, `newpay`. They can be (re)calculated from the
#' main NIS variables and perhaps should be, to check the validity of this 
#' script.
#' 
#' **Caveat**: `died` only reflects those patients whose discharge disposition 
#' indicates death, it ignores the `age_at_death_days` variable available in our
#' local data, for consistency with NIS. As mentioned above, `hhc` is always 
#' `0` because I don't know which of our codes would correspond to NIS's 
#' `6`.
dat00[,`:=`( died=1*(DISPUNIFORM=='20')
            ,hhc=1*(DISPUNIFORM=='6')
            ,routine=1*(DISPUNIFORM=='1')
            ,transferother=1*(DISPUNIFORM=='5')
            ,transfershort=1*(DISPUNIFORM=='2')
            ,hispanic=1*(RACE=='3')
            ,newpay=ifelse(PAY1 %in% 1:4,PAY1,'5')
)];

#' 
#' ***
#' #### Create the 2012 and 2013 years
dat2012 <- dat00[start_date < as.Date('2013-01-01')];
dat2013 <- dat00[!patient_num %in% dat2012$patient_num & 
                 start_date < as.Date('2014-01-01')];
#' ***
#' #### Summary
#' ***
#' #### Save files
export(dat2012[,..requestedcols],'dat2012.csv');
export(dat2013[,..requestedcols],'dat2013.csv');
c()

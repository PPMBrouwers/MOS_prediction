#---------------------------------------------------------------------------------------------------------------------------------------------------#
#CONNECT TO DATA VIA HIVE
#HAAL DATA OP PER DAG

options(scipen=999)

R_path = #something like: "C:/dev/R
result_path = #something like: "C:/dev/R/results"
setwd(R_path)
tac_filename = #something like: "tac_samsung_apple.csv"
TAClist <- data.frame(read.csv2(tac_filename))
SamsungIndex <- TAClist[,2]=="Samsung"
SamsungTAC <- TAClist[SamsungIndex,1]
AppleIndex <- TAClist[,2]=="Apple"
AppleTAC <- TAClist[AppleIndex,1]
TAClist <- paste(TAClist[,1], collapse=",")
TAClist <- paste("(",TAClist,")", sep="")
rm(SamsungIndex, AppleIndex)
library(plyr);
library(dplyr);
library(data.table); 
require(RJDBC); library(RJDBC)

starttime <- as.numeric(Sys.time())

tijdmax=as.POSIXct(Sys.time())-((3600*24)) #datum en tijd 24 uur terug

datummax=as.numeric(strftime(tijdmax, "%Y%m%d"))
datummin=datummax

cat(paste0("Processing day: ", datummax))

# HIVE IMPORT ---------------------------------------------------------------------------------------------------------------------------------------------------#
# Download the Hortonworks JDBC from http://public-repo-1.hortonworks.com/HDP/hive-jdbc4/1.0.36.1046/Simba_HiveJDBC41_1.0.36.1046.zip
# Extract and please the drivers in a local drive and provide the path of the library in the format as shown below
# Load Hive JDBC driver
hive_driver_details <- #something like: "com.simba.hive.jdbc41.HS2Driver"
driver_files <- #something like: c(list.files("C:/Program Files (x86)/R/R-3.x.x/library/Simba_HiveJDBCxx_x.x.xx.xxxx",pattern="jar$",full.names=TRUE)))
  
hivedrv <- JDBC(hive_driver_details,driver_files)
#Connect to Hive service
hive_security_details <- #something like: "jdbc:hive2://c99p01.udex.haas.local:9443/;ssl=1;
                         #                 SSLKeyStore=truststore_path;
                         #                 SSLKeyStorePwd=#your_SSLKeyStore_password;transportMode=http;httpPath=gateway/default/hive;AuthMech=x;
                         #                 AllowAllHostNames=x;UID=#your_uid;PWD=#your_password;AllowSelfSignedCerts=x;more_details?")


hivecon <- dbConnect(hivedrv, hive_security_details)
# Create a query for executing on hive
query <- paste0("
                SELECT
                interval_start_timestamp,
                first_packet_seen_timestamp,
                last_packet_seen_timestamp,
                first_packet_seen_in_interval_timestamp,
                last_packet_seen_in_interval_timestamp,
                imei,
                core_to_ue_rtt,
                tcp_rst,
                termination_cause,
                packets_uplink,
                bytes_uplink,
                bytes_downlink,
                service_id,
                substr(imei,0,8) as TAC
                FROM
                genes_staging.filteredstream 
                WHERE
                substr(imei,0,8) in ",TAClist,"
                AND date >= ",datummin,"
                AND date <= ",datummax,"
                UNION ALL
                SELECT
                interval_start_timestamp,
                first_packet_seen_timestamp,
                last_packet_seen_timestamp,
                first_packet_seen_in_interval_timestamp,
                last_packet_seen_in_interval_timestamp,
                imei,
                core_to_ue_rtt,
                tcp_rst,
                termination_cause,
                packets_uplink,
                bytes_uplink,
                bytes_downlink,
                service_id,
                substr(imei,0,8) as TAC
                FROM
                genes_staging.filteredstream_history 
                WHERE
                substr(imei,0,8) in ",TAClist,"
                AND date >= ",datummin,"
                AND date <= ",datummax,"
                ")

inputtime <- as.numeric(Sys.time())
inputdata <- dbGetQuery(hivecon, query) #output file
outputtime <- as.numeric(Sys.time()) - inputtime

names(inputdata) <- c("interval_start_timestamp", "first_packet_seen_timestamp","last_packet_seen_timestamp","first_packet_seen_in_interval_timestamp","last_packet_seen_in_interval_timestamp","imei", "core_to_ue_rtt","tcp_rst", "termination_cause","packets_uplink","bytes_uplink","bytes_downlink","service_id","TAC")

#---------------------------------------------------------------------------------------------------------------------------------------------------#
#Bereken MOS#

inputdata <- as.data.frame(inputdata[order(inputdata$interval_start_timestamp, inputdata$imei),]) #sort to have the first timeslot in the beginning
AnalysisFile <- data.table(inputdata[,c("interval_start_timestamp","imei")]) #kolommen die de row identificeren
AnalysisFile$Terminal <- as.factor(ifelse(inputdata$TAC %in% SamsungTAC, "Samsung", (ifelse(inputdata$TAC %in% AppleTAC, "Apple", "Other")))) #checking of de TAC is van Samsungphone of iPhone, hier zou niks 'other' moeten worden vanwege selectie in SQL

#Toevoegen relevante variabelen
AnalysisFile$core_to_ue_rtt <- inputdata$core_to_ue_rtt
AnalysisFile$tcp_rst <- inputdata$tcp_rst
AnalysisFile$termination_cause <- inputdata$termination_cause
AnalysisFile$packets_uplink <- inputdata$packets_uplink
AnalysisFile$bytes_uplink <- inputdata$bytes_uplink
AnalysisFile$Throughput_UL_1 <- ifelse((inputdata$last_packet_seen_timestamp - inputdata$first_packet_seen_timestamp) > 0, ((8*inputdata$bytes_uplink)/(1000*(inputdata$last_packet_seen_timestamp - inputdata$first_packet_seen_timestamp))), 0)
AnalysisFile$Throughput_UL_2 <- ifelse((inputdata$last_packet_seen_in_interval_timestamp - inputdata$first_packet_seen_in_interval_timestamp) > 0, ((8*inputdata$bytes_uplink)/(1000*(inputdata$last_packet_seen_in_interval_timestamp - inputdata$first_packet_seen_in_interval_timestamp))), 0)
AnalysisFile$Throughput_DL_2 <- ifelse((inputdata$last_packet_seen_in_interval_timestamp - inputdata$first_packet_seen_in_interval_timestamp) > 0, ((8*inputdata$bytes_downlink)/(1000*(inputdata$last_packet_seen_in_interval_timestamp - inputdata$first_packet_seen_in_interval_timestamp))), 0)
AnalysisFile$service_id <- inputdata$service_id
AnalysisFile <- AnalysisFile[AnalysisFile$Terminal != "Other",] #Only select Samsung and Apple phones

#formules
total <- 0
factor02<-0.2
factor5<-5
#onderstaande formule is nodig voor de Lp waarden
funfac <- function(x,y){  for (i in x){ (i <- i ** y);total <- total + i};return((abs(total/(length(x))))**(1/y));total=0 }

#Huidige frame berekening
CalculationsCurrentFrame <- AnalysisFile[,.(funfac(tcp_rst,factor5),sum(core_to_ue_rtt),length(termination_cause[termination_cause==2]),min(core_to_ue_rtt),sum(bytes_uplink),funfac(Throughput_UL_2,factor02),funfac(packets_uplink,factor02),(length(termination_cause[termination_cause==2])/length(termination_cause)),min(Throughput_DL_2),mean(Throughput_UL_1)),by=.(interval_start_timestamp,imei)]   # simplest
names(CalculationsCurrentFrame) <- c("interval_start_timestamp","imei", "tcp_rst_Int1_lp5", "core_to_ue_rtt_Int1_sum", "Termination_Cause_Int1_2_Count" ,"core_to_ue_rtt_Int1_min", "bytes_uplink_Int1_sum","Throughput_UL_2_Int1_lp02", "packets_uplink_Int1_lp02","Termination_Cause_Int1_2_Avg","Throughput_DL_2_Int1_min","Throughput_UL_1_Int1_avg")

#Youtube
YT <- setDT(AnalysisFile) 
YT <- YT[ , .(service_id = list(service_id)), by = .(interval_start_timestamp,imei)]
YT <- YT$service_id
#voeg in nieuwe kolom Youtube of service id van Youtube (1124) aanwezig is (1) toe aan Calculationscurrentframe.
CalculationsCurrentFrame$Youtube <- sapply(YT, function(x) (max(1124 %in% x)))
#Om naast Youtube ook QUIC toe te voegen gebruik deze commando
#CalculationsCurrentFrame$Youtube <- (sapply(YT, function(x) (max((1124) %in% x)+ max((1188) %in% x)))) #1124 is Youtube #1188 is QUIC

#In andere tijdslot aanwezig
dfs <- split(AnalysisFile$imei, AnalysisFile$interval_start_timestamp)
AnalysisFile$inMin1 <- unlist(mapply(`%in%`, dfs,  lag(dfs)))
AnalysisFile$inPlus1 <- unlist(mapply(`%in%`, dfs,  lead(dfs)))
dfs <- split(CalculationsCurrentFrame$imei, CalculationsCurrentFrame$interval_start_timestamp)
CalculationsCurrentFrame$plus1 <- unlist(mapply(`%in%`, dfs,  lead(dfs)))
CalculationsCurrentFrame$plus2 <- unlist(mapply(`%in%`, dfs, lead(dfs,2)))
CalculationsCurrentFrame$min1 <- unlist(mapply(`%in%`, dfs,  lag(dfs)))
CalculationsCurrentFrame$min2 <- unlist(mapply(`%in%`, dfs, lag(dfs,2)))
rm(dfs)

#Variabelen Samsung & Apple
#Variabele A.Samsung is in current
#Variabele B.Samsung
df1 <- CalculationsCurrentFrame %>%
  group_by(imei) %>% 
  mutate(core_to_ue_rtt_Int1_sum = ifelse(plus1&plus2, lead(core_to_ue_rtt_Int1_sum,2), ifelse(plus2,lead(core_to_ue_rtt_Int1_sum),NA))) 
core_to_ue_rtt_Int1_Nplus2_sum <- df1$core_to_ue_rtt_Int1_sum
#Variabele C.Samsung is in current
#Variabele D.Samsung is in current
#Variabele E.Samsung
df1 <- CalculationsCurrentFrame %>%
  group_by(imei) %>% 
  mutate(bytes_uplink_Int1_sum = ifelse(plus1&plus2, lead(bytes_uplink_Int1_sum,2), ifelse(plus2,lead(bytes_uplink_Int1_sum),NA))) 
bytes_uplink_Int1_Nplus2_sum <- df1$bytes_uplink_Int1_sum
#Variabele F.Samsung
df1 <- CalculationsCurrentFrame %>%
  group_by(imei) %>% 
  mutate(Throughput_UL_2_Int1_lp02 = ifelse(min1&min2, lag(Throughput_UL_2_Int1_lp02,2), ifelse(min2,lag(Throughput_UL_2_Int1_lp02),NA))) 
Throughput_UL_2_Int1_Nmin2_lp02 <- df1$Throughput_UL_2_Int1_lp02
#Variabele A.Apple 
df <- AnalysisFile %>% 
  group_by(interval_start_timestamp, imei) %>% 
  summarise(inPlus1 = all(inPlus1), s = sum(Throughput_UL_2^factor02), n = n()) %>% 
  group_by(imei) %>% 
  mutate(s1 = s + lead(s), n1 = n + lead(n), lp02 = ifelse(inPlus1, abs((s1/n1)^(1/factor02)), NA)) 
Throughput_UL_2_IntPlus2_Lp02 <- df$lp02
#Variabele B.Apple is in current
#Variabele C.Apple is in current
#Variabele D.Apple
df <- AnalysisFile %>% 
  group_by(interval_start_timestamp, imei) %>% 
  summarise(inMin1 = all(inMin1), s = sum(termination_cause^factor5), n = n()) %>% 
  group_by(imei) %>% 
  mutate(s1 = s + lag(s), n1 = n + lag(n), lp5 = ifelse(inMin1, abs((s1/n1)^(1/factor5)), NA)) 
termination_cause_IntMin2_lp5 <- df$lp5
#Variabele E.Apple
df1 <- CalculationsCurrentFrame %>%
  group_by(imei) %>% 
  mutate(Throughput_DL_2_Int1_min = ifelse(min1, lag(Throughput_DL_2_Int1_min),NA)) 
Throughput_DL_2_Int1_Nmin1_min <- df1$Throughput_DL_2_Int1_min
#Variabele F.Apple
df1 <- CalculationsCurrentFrame %>%
  group_by(imei) %>% 
  mutate(Throughput_UL_1_Int1_avg = ifelse(min1&min2, lag(Throughput_UL_1_Int1_avg,2), ifelse(min2,lag(Throughput_UL_1_Int1_avg),NA))) 
Throughput_UL_1_Int1_Nmin2_avg <- df1$Throughput_UL_1_Int1_avg
rm(df,df1)

#MOS Calculations
MOSscores <- data.frame(CalculationsCurrentFrame[,1:2])
MOSscores <- cbind(MOSscores, Terminal  = ifelse(substr(MOSscores$imei, 1, 8) %in% SamsungTAC, "Samsung", "Apple"));
MOSscores <- cbind(MOSscores, A.Samsung = CalculationsCurrentFrame$tcp_rst_Int1_lp5)
MOSscores <- cbind(MOSscores, B.Samsung = core_to_ue_rtt_Int1_Nplus2_sum)
MOSscores <- cbind(MOSscores, C.Samsung = CalculationsCurrentFrame$Termination_Cause_Int1_2_Count)
MOSscores <- cbind(MOSscores, D.Samsung = CalculationsCurrentFrame$core_to_ue_rtt_Int1_min)
MOSscores <- cbind(MOSscores, E.Samsung = bytes_uplink_Int1_Nplus2_sum)
MOSscores <- cbind(MOSscores, F.Samsung = Throughput_UL_2_Int1_Nmin2_lp02)
MOSscores$Youtube <- CalculationsCurrentFrame$Youtube

MOSscores <- na.omit(MOSscores[(MOSscores$Terminal=="Samsung"),])

MOSscorea <- data.frame(CalculationsCurrentFrame[,1:2])
MOSscorea <- cbind(MOSscorea, Terminal  = ifelse(substr(MOSscorea$imei, 1, 8) %in% AppleTAC, "Apple", "Samsung"));
MOSscorea <- cbind(MOSscorea, A.Apple = Throughput_UL_2_IntPlus2_Lp02)
MOSscorea <- cbind(MOSscorea, B.Apple = CalculationsCurrentFrame$packets_uplink_Int1_lp02)
MOSscorea <- cbind(MOSscorea, C.Apple = CalculationsCurrentFrame$Termination_Cause_Int1_2_Avg)
MOSscorea <- cbind(MOSscorea, D.Apple = termination_cause_IntMin2_lp5)
MOSscorea <- cbind(MOSscorea, E.Apple = Throughput_DL_2_Int1_Nmin1_min)
MOSscorea <- cbind(MOSscorea, F.Apple = Throughput_UL_1_Int1_Nmin2_avg)
MOSscorea$Youtube <- CalculationsCurrentFrame$Youtube

MOSscorea <- na.omit(MOSscorea[(MOSscorea$Terminal=="Apple"),])

#Samsung
MinSamsung1 <- 0;  MaxSamsung1 <- 1;            FloorSamsung1 <- 0; CeilSamsung1 <- 1;
MinSamsung2 <- 0;  MaxSamsung2 <- 95934;        FloorSamsung2 <- 0; CeilSamsung2 <- 20000;
MinSamsung3 <- 0;  MaxSamsung3 <- 47;           FloorSamsung3 <- 0; CeilSamsung3 <- 47;
MinSamsung4 <- 0;  MaxSamsung4 <- 75;           FloorSamsung4 <- 0; CeilSamsung4 <- 40;
MinSamsung5 <- 0;  MaxSamsung5 <- 8471105;      FloorSamsung5 <- 0; CeilSamsung5 <- 8471105;
MinSamsung6 <- 0;  MaxSamsung6 <- 0.138879523;  FloorSamsung6 <- 0; CeilSamsung6 <- 0.138879521;

floor1s <- function(x){if ( x < FloorSamsung1 ) {FloorSamsung1 } else {ifelse( x > CeilSamsung1, CeilSamsung1, x)}}
floor2s <- function(x){if ( x < FloorSamsung2 ) {FloorSamsung2 } else {ifelse( x > CeilSamsung2, CeilSamsung2, x)}}
floor3s <- function(x){if ( x < FloorSamsung3 ) {FloorSamsung3 } else {ifelse( x > CeilSamsung3, CeilSamsung3, x)}}
floor4s <- function(x){if ( x < FloorSamsung4 ) {FloorSamsung4 } else {ifelse( x > CeilSamsung4, CeilSamsung4, x)}}
floor5s <- function(x){if ( x < FloorSamsung5 ) {FloorSamsung5 } else {ifelse( x > CeilSamsung5, CeilSamsung5, x)}}
floor6s <- function(x){if ( x < FloorSamsung6 ) {FloorSamsung6 } else {ifelse( x > CeilSamsung6, CeilSamsung6, x)}}

MOSscores$A.Samsung <- as.numeric(lapply(MOSscores$A.Samsung, floor1s))
MOSscores$B.Samsung <- as.numeric(lapply(MOSscores$B.Samsung, floor2s))
MOSscores$C.Samsung <- as.numeric(lapply(MOSscores$C.Samsung, floor3s))
MOSscores$D.Samsung <- as.numeric(lapply(MOSscores$D.Samsung, floor4s))
MOSscores$E.Samsung <- as.numeric(lapply(MOSscores$E.Samsung, floor5s))
MOSscores$F.Samsung <- as.numeric(lapply(MOSscores$F.Samsung, floor6s))

I1s <- (MOSscores$A.Samsung-MinSamsung1)/(MaxSamsung1-MinSamsung1);
I2s <- (MOSscores$B.Samsung-MinSamsung2)/(MaxSamsung2-MinSamsung2);
I3s <- (MOSscores$C.Samsung-MinSamsung3)/(MaxSamsung3-MinSamsung3);
I4s <- (MOSscores$D.Samsung-MinSamsung4)/(MaxSamsung4-MinSamsung4);
I5s <- (MOSscores$E.Samsung-MinSamsung5)/(MaxSamsung5-MinSamsung5);
I6s <- (MOSscores$F.Samsung-MinSamsung6)/(MaxSamsung6-MinSamsung6);
A1s <- 6.90160046907025;
A2s <- -7.38929787066861;
A3s <- 1.77925314050863;
A4s <- -6.32322656113755;
A5s <- -3.64919485344828;
A6s <- -1.66617114145528;
A7s <- -3.28782329141106;
B1s <- 6.31788610834767;
B2s <- -6.83910061691106;
B3s <- 26.5841340623315;
B4s <- -5.28347223573713;
B5s <- -2.22957305562301;
B6s <- -1.60095239502786;
B7s <- -3.35295467589783;
As <- A1s + A2s*I1s + A3s*I2s + A4s*I3s + A5s*I4s + A6s*I5s + A7s*I6s;
Bs <- B1s + B2s*I1s + B3s*I2s + B4s*I3s + B5s*I4s + B6s*I5s + B7s*I6s;
T1s <- 2.93454518940626;
T2s <- 3.98712739205855;
MOS <- T1s + T2s * As * Bs / ( As**2 + Bs**2 );
moslimit <- function(x) {if ( x > 5 ) {5} else {ifelse( x <1, 1, x)}}
MOSscores$MOS <- as.numeric(lapply(MOS,moslimit))

#Apple
MinApple1 <- 0;             MaxApple1 <- 0.085599571;       FloorApple1 <- 0.0015; CeilApple1 <- 0.06;
MinApple2 <- 0;             MaxApple2 <- 543.8372192;       FloorApple2 <- 0;                            CeilApple2 <- 400;
MinApple3 <- 0;             MaxApple3 <- 0.857142866;       FloorApple3 <- 0;                            CeilApple3 <- 0.857142857;
MinApple4 <- 0;             MaxApple4 <- 2.712029934;       FloorApple4 <- 1;                            CeilApple4 <- 2.2;
MinApple5 <- 0;             MaxApple5 <- 0.147200003;       FloorApple5 <- 0;                            CeilApple5 <- 0.1472;
MinApple6 <- 0;             MaxApple6 <- 0.173482344;       FloorApple6 <- 0;                            CeilApple6 <- 0.14;

floor1a <- function(x){if ( x < FloorApple1 ) {FloorApple1 } else {ifelse( x > CeilApple1, CeilApple1, x)}}
floor2a <- function(x){if ( x < FloorApple2 ) {FloorApple2 } else {ifelse( x > CeilApple2, CeilApple2, x)}}
floor3a <- function(x){if ( x < FloorApple3 ) {FloorApple3 } else {ifelse( x > CeilApple3, CeilApple3, x)}}
floor4a <- function(x){if ( x < FloorApple4 ) {FloorApple4 } else {ifelse( x > CeilApple4, CeilApple4, x)}}
floor5a <- function(x){if ( x < FloorApple5 ) {FloorApple5 } else {ifelse( x > CeilApple5, CeilApple5, x)}}
floor6a <- function(x){if ( x < FloorApple6 ) {FloorApple6 } else {ifelse( x > CeilApple6, CeilApple6, x)}}

MOSscorea$A.Apple <- as.numeric(lapply(MOSscorea$A.Apple, floor1a))
MOSscorea$B.Apple <- as.numeric(lapply(MOSscorea$B.Apple, floor2a))
MOSscorea$C.Apple <- as.numeric(lapply(MOSscorea$C.Apple, floor3a))
MOSscorea$D.Apple <- as.numeric(lapply(MOSscorea$D.Apple, floor4a))
MOSscorea$E.Apple <- as.numeric(lapply(MOSscorea$E.Apple, floor5a))
MOSscorea$F.Apple <- as.numeric(lapply(MOSscorea$F.Apple, floor6a))

I1a <- (MOSscorea$A.Apple-MinApple1)/(MaxApple1-MinApple1);
I2a <- (MOSscorea$B.Apple-MinApple2)/(MaxApple2-MinApple2);
I3a <- (MOSscorea$C.Apple-MinApple3)/(MaxApple3-MinApple3);
I4a <- (MOSscorea$D.Apple-MinApple4)/(MaxApple4-MinApple4);
I5a <- (MOSscorea$E.Apple-MinApple5)/(MaxApple5-MinApple5);
I6a <- (MOSscorea$F.Apple-MinApple6)/(MaxApple6-MinApple6);

A1a <- 0.840464536383483;
A2a <- -0.56784603655317;
A3a <- 4.48469393397661;
A4a <- -148.549072968817;
A5a <- -2.02642506144836;
A6a <- -1190.39290461116;
A7a <- -1.93778978945023;
B1a <- -4.75276458213046;
B2a <- 5.31709191240059;
B3a <- -7.15990912808544;
B4a <- -36.1755502064987;
B5a <- 5.57147251385083;
B6a <- 934.463457860107;
B7a <- 2.50567314126991;

Aa <- A1a + A2a*I1a + A3a*I2a + A4a*I3a + A5a*I4a + A6a*I5a + A7a*I6a;
Ba <- B1a + B2a*I1a + B3a*I2a + B4a*I3a + B5a*I4a + B6a*I5a + B7a*I6a;
T1a <- 3.04806894046034;
T2a <- -4.00253000249366;
MOS <- T1a + T2a * Aa * Ba / ( Aa**2 + Ba**2 );
MOSscorea$MOS <- as.numeric(lapply(MOS,moslimit))

#MOSscore
MOSscore <- full_join(MOSscorea,MOSscores)  #outer join
MOSscore <- MOSscore[order(MOSscore$interval_start_timestamp, MOSscore$imei),] #sort to have the first timeslot in the beginning
MOSscoreYt <- MOSscore[MOSscore$Youtube != 0,c(1:3,11,4:9,12:17)] #identifiers, MOS, Apple variables, Samsung variables
names(MOSscoreYt) <- c("interval_start_timestamp", "imei", "Terminal", "MOS", "Throughput_UL_2_IntPlus2_Lp02", "packets_uplink_Int1_Lp02", "Termination_Cause_Int1_2_Avg", "termination_cause_IntMin2_lp5", "Throughput_DL_2_Int1_Nmin1_min", "Throughput_DL_2_Int1_Nmin1_min","tcp_rst_int1_lp5", "core_to_ue_rtt_Int1_Nplus2_sum", "Termination_Cause_Int1_2_Count", "core_to_ue_rtt_Int1_min", "bytes_uplink_Int1_Nplus2_sum", "Throughput_UL_2_Int1_Nmin2_lp02")

#voeg MOS score en variabelen toe aan originele tabel, filter vervolgens op Youtube (1124) en QUIC (1188)
inputwithMOS = left_join(AnalysisFile, MOSscoreYt, by = c("interval_start_timestamp","imei"))
inputwithMOS_YT = inputwithMOS[!is.na(inputwithMOS$MOS),c(1:3,14,4:13,15:26)]
MOSscoreYT = inputwithMOS_YT[inputwithMOS_YT$service_id == (1124) | inputwithMOS_YT$service_id == (1188),] 
#maak strings van imei en tijdslot voor datameer join 
MOSscoreYt$interval_start_timestamp <- as.character(MOSscoreYt$interval_start_timestamp)
MOSscoreYt$imei <- as.character(MOSscoreYt$imei)


setwd(result_path)
write.table(MOSscoreYt,file=paste0("MOSYT_YYYYMMDD.csv"),sep=";",row.names = FALSE,na="") 

totaltime <- as.numeric(Sys.time()) - starttime
cat(paste0("Total runtime of this script was: ",as.integer(totaltime/60)," minutes."))
cat(paste0("importing the data costs: ",as.integer(outputtime/60)," minutes."))
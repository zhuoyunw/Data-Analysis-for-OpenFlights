/* Group Member: Qinxiao Zhang, Zhuoyun Wang */

libname Project "/home/zhuoyun20/sasuser.v94/Project";

data airlines;
infile "/home/zhuoyun20/sasuser.v94/Project/airlines.dat.txt" dlm = ',' dsd missover firstobs = 2 ;
input ID  Name :$50. alias $ code2 $ code3 $ callsign :$20. Country :$20. Active $;
run;

proc contents data=airlines;
run;

proc means data=airlines n nmiss;
var ID;
run;

data work.airlines_cleaned;
set airlines;
where Active = 'Y' and 
      ID ^= .;    
run;

proc contents data=airlines_cleaned;
run;

data airports;
infile "/home/zhuoyun20/sasuser.v94/Project/airports.dat.txt" dlm = ',' dsd  flowover;
retain Airport_ID Name City Country code3 code4 lat long al timezone dst tz type source;
input Airport_ID Name :$50. City :$30. Country :$30. code3 $ code4 $ 
		lat long  al timezone :$6. dst :$2. tz :$30. type :$15. source :$15.;
run;

proc contents data=airports;
run;

proc means data=airports n nmiss;
var Airport_ID;
run;

data routes;
infile "/home/zhuoyun20/sasuser.v94/Project/routes.dat.txt" dlm = ',' dsd  missover;
input code :$3. ID $ source :$5. sourceID $ dest :$5. destID $ share :$1. 
		stops equipment $;
run;

proc contents data=routes;
run;

data a;
set routes;
where ID = '\N' or sourceID = '\N' or destID = '\N';
run;

proc contents data=a;
run;

data routes_cleaned;
set routes;
where sourceID ^= '\N' and
      ID ^= '\N' and
      destID ^= '\N';
ID_airline = input(ID, 8.);
source_ID = input(sourceID, 8.);
dest_ID = input(destID, 8.);
drop ID sourceID destID;
;
run;

proc contents data=routes_cleaned;
run;

proc means data=routes_cleaned n nmiss;
var ID_airline source_ID dest_ID;
run;

/* which active airline(s) had the most route records */
proc sql;
create table max_routes as
select airlines_cleaned.ID, Name, Country, source, source_ID, dest, dest_ID
from airlines_cleaned, routes_cleaned
where airlines_cleaned.ID = routes_cleaned.ID_airline
;
quit;

proc sql;
create table ordered_id as
select ID, count(ID) as ID_frequency
from max_routes 
group by ID
order by ID_frequency desc
;                             
quit;

data ordered_id_most;
set ordered_id (obs=3);
run;

proc sql;
select distinct ordered_id_most.ID, Name, Country, ID_frequency
from ordered_id_most left join max_routes
on ordered_id_most.ID = max_routes.ID
order by ID_frequency desc
;
title 'Top 3 Active Airlines with the Most Route Records';
title;
quit;




/* most popular sources */
proc sql;
create table popular_airports as 
select airports.Airport_ID, airports.Name, airports.City, 
       airports.Country, routes_cleaned.source, routes_cleaned.source_ID
from airports, routes_cleaned
where airports.Airport_ID = routes_cleaned.source_ID
;
quit;

proc sql;
create table ordered_airports as
select source_ID, count(source_ID) as Source_Freq
from popular_airports
group by source_ID
order by Source_Freq desc
;
quit;

data ordered_airports_max;
set ordered_airports (obs=3);
run;

proc sql;
select distinct ordered_airports_max.source_ID, 
                Name, City, Country, source, Source_Freq
from ordered_airports_max left join popular_airports
on ordered_airports_max.source_ID = popular_airports.Airport_ID
order by Source_Freq desc
;
title 'Top 3 Source Airports';
quit;
title;



/* non-stop and one-stop flight percentage */
proc sql;
create table stops as
select count(stops) as Count_Stops, stops
from routes_cleaned
where stops ^= .
group by stops
;
quit;

proc sql;
select Count_Stops / sum(Count_Stops) as Percentage format=percent12.4, 
       stops
from stops
;
quit;

/* active airlines with one-stop flights */
proc sql;
select id, name, country
from airlines_cleaned
where id in 
(select id_airline
from routes_cleaned
where stops = 1)
;
quit;



/* O'Hare Airport */
proc sql ;
create table ohare_dest as 
select source_ID, dest, count(dest) as ohare_dcount
from routes_cleaned
where source_ID in (select Airport_ID 
					from airports
					where Name like "% O'Hare %")
group by source_ID, dest
order by ohare_dcount desc;

create table ohare_airline as
select source_ID,  ID_airline, count(ID_airline) as ohare_idcount
from routes_cleaned
where source_ID in (select Airport_ID 
					from airports
					where Name like "% O'Hare %")
group by source_ID, ID_airline
order by ohare_idcount desc;
quit;

data ohare_dest3;
set ohare_dest (obs=3);
run;
data ohare_id3;
set ohare_airline (obs=3);
run;
data ohare;
retain Name dest dest_count id id_count;
merge ohare_dest3 ohare_id3;
by source_ID;
Name = "Chicago O'Hare International Airport";
run;

proc sql;
create table Ohare_pop as 
select ohare.Name, dest label = "Destination", ohare_dcount label = "Count", air.Name as air_name, ohare_idcount label = "Airline Count"
from ohare as o left join airlines_cleaned as air
			on o.id_airline = air.id
;
quit;
proc sql;
select *
from ohare_pop;
quit;


/* Midway airport */
/*proc print data=airports;
where Name contains "Midway" or Name contains "O'Hare";
run;*/

proc sql ;
create table mid_dest as
select source_ID, dest, count(dest) as dest_count
from routes_cleaned
where source_ID in (select Airport_ID 
					from airports
					where Name like "% Midway %")
group by source_ID, dest
order by dest_count desc;


create table mid_airline as
select source_ID,  ID_airline, count(ID_airline) as id_count
from routes_cleaned
where source_ID in (select Airport_ID 
					from airports
					where Name like "% Midway %")
group by source_ID, ID_airline
order by id_count desc;
quit;


data midway_dest3;
set mid_dest (obs=3);
run;
data midway_id3;
set mid_airline (obs=3);
run;
data midway;
retain Name dest dest_count id id_count;
merge midway_dest3 midway_id3;
by source_ID;
Name = 'Chicago Midway International Airport';
run;

proc sql;
create table Mid_pop as 
select midway.Name, dest label = "Destination", dest_count label = "Count", air.Name as air_name label =  "Airline Name", id_count label = "Airline Count"
from midway as m left join airlines_cleaned as air
			on m.id_airline = air.id
;
quit;

proc sql;
select *
from Mid_pop;
title "Chicago Midway International Airport Top3 Destinations and Airlines";
quit;
title;


























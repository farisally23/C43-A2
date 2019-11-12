set search_path to a2;

CREATE VIEW WinLossTable2014 AS
(
select 
	winner as pname, 
	wins.courtname as court, 
	wins.year as year,
	wins,
	losses
from

	(select pname as winner, courtname, year, count(*) as wins
	from event
	join player
		on winid = pid
	join court
		on court.courtid = event.courtid
	where year = 2014
	group by pname, courtname, year) wins

left join

	(
	select pname as loser, courtname, year, count(*) as losses
	from event
	join player
		on lossid = pid
	join court
		on court.courtid = event.courtid
	where year = 2014
	group by pname, courtname, year) losses

on losses.loser = wins.winner and losses.courtname = wins.courtname);

CREATE VIEW Played200MinutesAvg AS
(
	select pname, avg(duration)
	from
	(
		select pname, duration
		from event
		join player
			on lossid = pid
		UNION ALL
		select pname, duration
		from event
		join player
			on winid = pid
	) timeStats
	group by pname
	having avg(duration) > 200
);

select distinct pname
from WinLossTable2014
where pname not in 
	(select pname
	from WinLossTable2014
	where losses >= wins)
and pname in 
	(select pname
	from Played200MinutesAvg)
order by pname desc;
SET search_path TO A2;

--Query 1 
INSERT INTO query1
(
select pname, cname, tname
from tournament 
join champion 
	on tournament.tid = champion.tid
join player
	on champion.pid = player.pid
join country
	on tournament.cid = country.cid
where player.cid = tournament.cid
order by pname asc
);

--Query 2
INSERT INTO query2
(
select tname, sum(capacity) as totalCapacity
from tournament 
join court 
	on tournament.tid = court.tid
group by tournament.tid
having sum(capacity) =
	(select max(seats) from
		(select tournament.tid, sum(capacity) as seats
		from tournament 
		join court 
			on tournament.tid = court.tid
		group by tournament.tid) tourneySeats
		)
order by tname asc
);

--View used for Query 3
CREATE VIEW matchups AS
(
select
	player as playerid,
	pid as opponentid,
	pname as opponentname, 
	globalrank as opponentrank 
from
(
	select winid as player, lossid as opponent
	from event
	UNION
	select lossid, winid
	from event
) matchups
left join player on pid = opponent
);

--Query 3
INSERT INTO query3
(
select p1id, p1name, pid as p2id, pname as p2name from
	(select playerid as p1id, pname as p1name, highest from
	(
		select playerid, min(opponentrank) as highest
		from matchups 
		group by playerid
	) bestOpponent
	join player
		on playerid = pid) playerInfo
join player
	on globalrank = highest
order by p1name asc
);

--Drop view for Query 3
DROP VIEW matchups;

--Query 4
INSERT INTO query4
(
select distinct player.pid, pname 
from champion
join player
	on champion.pid = player.pid
group by player.pid, pname
having count (distinct tid) in
	(select count(distinct tid) from tournament)
order by pname asc
);

--Query 5
INSERT INTO query5
(
select
	player.pid,
	pname, 
	cast(sum(wins) as real)/count(distinct year) as avgwins
from player 
join record 
	on player.pid = record.pid
where year between 2011 and 2014
group by player.pid,pname
order by sum(wins)/count(distinct year) DESC
limit 10
);

--Views for Query 6
CREATE VIEW wins2011 AS
(
select player.pid as pid, pname, wins
from player 
join record 
	on player.pid = record.pid
where year = 2011
);
CREATE VIEW wins2012 AS
(
select player.pid as pid, pname, wins
from player 
join record 
	on player.pid = record.pid
where year = 2012
);
CREATE VIEW wins2013 AS
(
select player.pid as pid, pname, wins
from player 
join record 
	on player.pid = record.pid
where year = 2013
);
CREATE VIEW wins2014 AS
(
select player.pid as pid, pname, wins
from player 
join record 
	on player.pid = record.pid
where year = 2014
);
--Query 6
INSERT INTO query6
(
select
	wins2011.pid,
	wins2011.pname
from wins2011
join wins2012 on wins2011.pid = wins2012.pid
join wins2013 on wins2011.pid = wins2013.pid
join wins2014 on wins2011.pid = wins2014.pid
where
(wins2012 > wins2011) and (wins2013 > wins2012) and (wins2014 > wins2013)
order by pname asc
);

--Drop Views for Query 6
DROP VIEW wins2011;
DROP VIEW wins2012;
DROP VIEW wins2013;
DROP VIEW wins2014;

--Query 7
INSERT INTO query7
(
select pname, year
from champion 
join player
	on player.pid = champion.pid
group by pname, year
having count(year) >= 2
order by pname desc, year desc
);

--Query 8
INSERT INTO query8
(
select p1name, pname as p2name, cname
from
(
	select pid, pname as p1name, cid as p1Country, opponentid 
	from
	(
		select winid as playerid, lossid as opponentid
		from event
		UNION
		select lossid, winid
		from event
	) matchups
	join player
		on pid = playerid
) p1Info
join player
	on player.pid = opponentid
join country
	on country.cid = player.cid
where p1Country = player.cid
order by cname asc, p1name desc
);

--Query 9
INSERT INTO query9
(
select cname, count(*) as champions
from player
join champion
	on champion.pid = player.pid
join country
	on player.cid = country.cid
group by cname
having count(*) =
	(select max(championships) from
	(
		select count(*) as championships
		from player
		join champion
			on champion.pid = player.pid
		join country
			on player.cid = country.cid
	group by cname) countries
	)
order by cname desc
);

--Views for Query 10
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
--Query 10
INSERT INTO query10
(
select distinct pname
from WinLossTable2014
where pname not in 
	(select pname
	from WinLossTable2014
	where losses >= wins)
and pname in 
	(select pname
	from Played200MinutesAvg)
order by pname desc
);

--Drop views for Query 10
DROP VIEW WinLossTable2014;
DROP VIEW Played200MinutesAvg;
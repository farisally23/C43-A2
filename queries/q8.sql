set search_path to a2;


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
order by cname asc, p1name desc;
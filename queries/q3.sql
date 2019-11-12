set search_path to a2;

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
order by p1name asc;
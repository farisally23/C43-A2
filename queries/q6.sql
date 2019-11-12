set search_path to a2;

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

select
	wins2011.pid,
	wins2011.pname
from wins2011
join wins2012 on wins2011.pid = wins2012.pid
join wins2013 on wins2011.pid = wins2013.pid
join wins2014 on wins2011.pid = wins2014.pid
where
(wins2012 > wins2011) and (wins2013 > wins2012) and (wins2014 > wins2013)
order by pname asc;

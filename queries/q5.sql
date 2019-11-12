set search_path to a2;

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
limit 10;
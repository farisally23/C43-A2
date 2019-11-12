set search_path to a2;

select pname, year
from champion 
join player
	on player.pid = champion.pid
group by pname, year
having count(year) >= 2
order by pname desc, year desc;


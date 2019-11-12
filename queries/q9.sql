set search_path to a2;

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
order by cname desc;
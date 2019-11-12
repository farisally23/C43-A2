set search_path to a2;

SELECT DISTINCT player.pid, pname 
FROM champion
JOIN player
	on champion.pid = player.pid
GROUP BY player.pid, pname
HAVING count (DISTINCT tid) IN
	(SELECT count(DISTINCT tid) from tournament)
ORDER BY pname ASC;
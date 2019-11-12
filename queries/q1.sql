set search_path to A2;

SELECT pname, cname, tname
FROM tournament 
JOIN champion 
	ON tournament.tid = champion.tid
JOIN player
	ON champion.pid = player.pid
JOIN country
	ON tournament.cid = country.cid
WHERE player.cid = tournament.cid
ORDER BY pname ASC;
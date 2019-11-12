set search_path to a2;

SELECT tname, sum(capacity) as totalCapacity
FROM tournament 
JOIN court 
	on tournament.tid = court.tid
GROUP BY tournament.tid
HAVING sum(capacity) =
	(SELECT max(seats) from
		(SELECT tournament.tid, sum(capacity) AS seats
		FROM tournament 
		JOIN court 
			on tournament.tid = court.tid
		GROUP BY tournament.tid) tourneySeats
		)
ORDER BY tname ASC;

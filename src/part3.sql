-- №1
DROP FUNCTION IF EXISTS fn_human_readable_TransferredPoints_table();

CREATE FUNCTION fn_human_readable_TransferredPoints_table()
    RETURNS TABLE
            (
                Peer1         varchar,
                Peer2         varchar,
                PointsAmount_ integer
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT tp1.checkingpeer,
                         tp1.checkedpeer,
                         CASE
                             WHEN (tp1.pointsamount < tp2.pointsamount)
                                 THEN (tp1.pointsamount * -1)
                             ELSE tp1.pointsamount END
                    FROM transferredpoints tp1
                             LEFT JOIN
                         transferredpoints tp2
                         ON tp2.checkingpeer = tp1.checkedpeer AND tp2.checkedpeer = tp1.checkingpeer);
END;
$$ LANGUAGE 'plpgsql';

SELECT *
  FROM fn_human_readable_TransferredPoints_table();




-- №2
DROP FUNCTION IF EXISTS fn_successfully_passed_the_check();

CREATE FUNCTION fn_successfully_passed_the_check()
    RETURNS TABLE
            (
                Peer varchar,
                Task varchar,
                XP   integer
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT ch.peer, ch.task, xpamount
                    FROM checks ch
                             JOIN xp x ON ch.id = x.checkid
                             JOIN p2p p ON ch.id = p.checkid
                   WHERE state = 'Success');
END;
$$ LANGUAGE 'plpgsql';

SELECT *
  FROM fn_successfully_passed_the_check();




-- №3
DROP FUNCTION IF EXISTS fn_who_have_not_left_campus(input_date date);

CREATE FUNCTION fn_who_have_not_left_campus(input_date date)
    RETURNS TABLE
            (
                Peers varchar
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT peer
                    FROM (SELECT peer, COUNT(*) ct
                            FROM timetracking
                           WHERE date = input_date
                             AND state = 2
                           GROUP BY peer) AS p
                   WHERE ct < 2);
END;
$$ LANGUAGE 'plpgsql';

SELECT *
  FROM fn_who_have_not_left_campus('2022-03-22');




-- №4
DROP PROCEDURE IF EXISTS Successful_Unsuccessful_Checks(IN ref refcursor);

CREATE PROCEDURE Successful_Unsuccessful_Checks(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT ROUND((cch1.Count_checks * 1.0 / (cch1.Count_checks * 1.0 + cch2.Count_checks * 1.0)) * 100.0,
                     2) SuccessfulChecks,
               ROUND((cch2.Count_checks * 1.0 / (cch1.Count_checks * 1.0 + cch2.Count_checks * 1.0)) * 100.0,
                     2) UnsuccessfulChecks
          FROM (SELECT state, COUNT(state) Count_checks
                  FROM checks
                           JOIN p2p p1 ON checks.id = p1.checkid
                 GROUP BY p1.state) AS cch1,
               (SELECT state, COUNT(state) Count_checks
                  FROM checks
                           JOIN p2p p1 ON checks.id = p1.checkid
                 GROUP BY p1.state) AS cch2
         WHERE cch1.state = 'Success'
           AND cch2.state = 'Failure';
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL Successful_Unsuccessful_Checks('cursor_4');
FETCH ALL IN "cursor_4";
COMMIT;




-- №5
DROP PROCEDURE IF EXISTS the_change_in_the_number_of_peer_points_1(IN ref refcursor);

CREATE PROCEDURE the_change_in_the_number_of_peer_points_1(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT checkingpeer Peer, SUM(transferred_point) PointsChange
          FROM ((SELECT checkingpeer, (SUM(pointsamount) * -1) transferred_point
                   FROM transferredpoints
                  GROUP BY checkingpeer)
           UNION ALL
          (SELECT checkedpeer, (SUM(pointsamount)) accepted_point
             FROM transferredpoints
            GROUP BY checkedpeer)) AS res_table
         GROUP BY checkingpeer;
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL the_change_in_the_number_of_peer_points_1('cursor_5');
FETCH ALL IN "cursor_5";
COMMIT;




-- №6
DROP PROCEDURE IF EXISTS the_change_in_the_number_of_peer_points_2(IN ref refcursor);

CREATE PROCEDURE the_change_in_the_number_of_peer_points_2(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT Peer1 Peer, SUM(transferred_point) PointsChange
          FROM ((SELECT Peer1, SUM(PointsAmount_) transferred_point
                   FROM (SELECT Peer1,
                                CASE
                                    WHEN (PointsAmount_ > 0) THEN PointsAmount_ * -1
                                    ELSE PointsAmount_ END AS PointsAmount_
                           FROM fn_human_readable_TransferredPoints_table()) AS t1
                  GROUP BY Peer1)
           UNION ALL
          (SELECT Peer2, SUM(PointsAmount_) accepted_point
             FROM (SELECT Peer2,
                          CASE WHEN (PointsAmount_ < 0) THEN PointsAmount_ * -1 ELSE PointsAmount_ END AS PointsAmount_
                     FROM fn_human_readable_TransferredPoints_table()) AS t2
            GROUP BY Peer2)) AS res_table
         GROUP BY Peer1;
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL the_change_in_the_number_of_peer_points_2('cursor_6');
FETCH ALL IN "cursor_6";
COMMIT;




-- №7
DROP PROCEDURE IF EXISTS the_most_frequently_checked_task_for_each_day(IN ref refcursor);

CREATE PROCEDURE the_most_frequently_checked_task_for_each_day(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT c.date Date, c.task Task
          FROM (SELECT date, task, COUNT(*) count FROM checks GROUP BY date, task) AS c
                   LEFT JOIN (SELECT date, task, COUNT(*) count
                                FROM checks
                               GROUP BY DATE, task) AS c2 ON c.date = c2.date AND c.count < c2.count
         WHERE c2.date IS NULL
         ORDER BY c.date;
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL the_most_frequently_checked_task_for_each_day('cursor_7');
FETCH ALL IN "cursor_7";
COMMIT;




-- №8
DROP PROCEDURE IF EXISTS duration_of_the_last_P2P_check(IN ref refcursor);

CREATE PROCEDURE duration_of_the_last_P2P_check(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT (TE.time - TS.time)::time duration
          FROM (SELECT time FROM p2p WHERE checkid = (SELECT MAX(checkid) FROM p2p) AND state = 'Start') AS TS,
               (SELECT time
                  FROM p2p
                 WHERE checkid = (SELECT MAX(checkid) FROM p2p)
                   AND (state = 'Success' OR state = 'Fail')) AS TE;
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL duration_of_the_last_P2P_check('cursor_8');
FETCH ALL IN "cursor_8";
COMMIT;




-- №9
CALL InsertIntoP2P('Sivana', 'Leon', 'CPP_Task_2', 'Start', '11:00:00');
CALL InsertIntoP2P('Sivana', 'Leon', 'CPP_Task_2', 'Success', '11:13:00');
CALL InsertIntoP2P('Bumbum', 'Sivana', 'SQL_Task_3', 'Start', '08:00:00');
CALL InsertIntoP2P('Bumbum', 'Sivana', 'SQL_Task_3', 'Success', '08:10:00');
CALL InsertIntoP2P('Bumbum', 'Mzoraida', 'SQL_Task_4', 'Start', '11:00:00');
CALL InsertIntoP2P('Bumbum', 'Mzoraida', 'SQL_Task_4', 'Success', '11:30:00');
CALL InsertIntoP2P('Bumbum', 'Leon', 'SQL_Task_5', 'Start', '17:00:00');
CALL InsertIntoP2P('Bumbum', 'Leon', 'SQL_Task_5', 'Success', '17:35:00');
CALL InsertIntoVerter('Sivana', 'CPP_Task_2', 'Start', '11:14:00');
CALL InsertIntoVerter('Sivana', 'CPP_Task_2', 'Success', '11:15:00');
CALL InsertIntoVerter('Bumbum', 'SQL_Task_3', 'Start', '08:11:00');
CALL InsertIntoVerter('Bumbum', 'SQL_Task_3', 'Success', '08:12:00');
CALL InsertIntoVerter('Bumbum', 'SQL_Task_4', 'Start', '11:31:00');
CALL InsertIntoVerter('Bumbum', 'SQL_Task_4', 'Success', '11:32:00');
CALL InsertIntoVerter('Bumbum', 'SQL_Task_5', 'Start', '17:36:00');
CALL InsertIntoVerter('Bumbum', 'SQL_Task_5', 'Success', '17:37:00');

UPDATE checks
   SET date = '2023-01-05'
 WHERE id = 12;
UPDATE checks
   SET date = '2023-01-29'
 WHERE id = 13;
UPDATE checks
   SET date = '2023-02-15'
 WHERE id = 14;

DROP PROCEDURE IF EXISTS completed_block_of_tasks(IN ref refcursor, name_of_the_block varchar);

CREATE PROCEDURE completed_block_of_tasks(IN ref refcursor, name_of_the_block varchar)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT DISTINCT count_task_from_block.peer, date
          FROM (SELECT peer, COUNT(*) count_task
                  FROM (SELECT DISTINCT peer, task
                          FROM checks
                                   JOIN p2p p ON checks.id = p.checkid
                                   JOIN verter v ON checks.id = v.checkid
                         WHERE p.state = 'Success'
                           AND v.state = 'Success'
                           AND task LIKE '%' || name_of_the_block || '%') AS peer_task
                 GROUP BY peer) AS count_task_from_block
                   JOIN checks ch2 ON ch2.peer = count_task_from_block.peer AND count_task = 3
              AND task LIKE '%' || name_of_the_block || '%' AND date = (SELECT MAX(date)
                                                                          FROM checks
                                                                         WHERE ch2.peer = count_task_from_block.peer
                                                                           AND task LIKE '%' || name_of_the_block || '%');
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL completed_block_of_tasks('cursor_9', 'SQL');
FETCH ALL IN "cursor_9";
COMMIT;




-- №10
DROP PROCEDURE IF EXISTS determine_recommended_peer(IN ref refcursor);

CREATE PROCEDURE determine_recommended_peer(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT Peer, RecommendedPeer
          FROM (SELECT Peer, RecommendedPeer, COUNT(*) count
                  FROM ((SELECT peer1 Peer, r.recommendedpeer RecommendedPeer
                           FROM friends
                                    LEFT JOIN recommendations r ON friends.peer2 = r.peer
                          WHERE peer1 != r.recommendedpeer)
                   UNION ALL
                  (SELECT peer2, r.recommendedpeer
                     FROM friends
                              LEFT JOIN recommendations r ON friends.peer1 = r.peer
                    WHERE peer2 != r.recommendedpeer)) AS t1
                 GROUP BY t1.Peer, t1.Recommendedpeer
                 ORDER BY t1.Peer) AS peer_count
         WHERE count = (SELECT MAX(count)
                          FROM (SELECT COUNT(*) count
                                  FROM ((SELECT peer1 Peer, r.recommendedpeer RecommendedPeer
                                           FROM friends
                                                    LEFT JOIN recommendations r ON friends.peer2 = r.peer
                                          WHERE peer1 != r.recommendedpeer)
                                   UNION ALL
                                  (SELECT peer2, r.recommendedpeer
                                     FROM friends
                                              LEFT JOIN recommendations r ON friends.peer1 = r.peer
                                    WHERE peer2 != r.recommendedpeer)) AS t1
                                 WHERE t1.Peer = peer_count.Peer
                                 GROUP BY t1.Peer, t1.Recommendedpeer
                                 ORDER BY t1.Peer) AS t2);
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL determine_recommended_peer('cursor_10');
FETCH ALL IN "cursor_10";
COMMIT;




-- №11
DROP PROCEDURE IF EXISTS the_percentage_of_peers(IN ref refcursor, block_1 varchar, block_2 varchar);

CREATE PROCEDURE the_percentage_of_peers(IN ref refcursor, block_1 varchar, block_2 varchar)
AS
$$
BEGIN
    OPEN ref FOR
        VALUES ((SELECT ROUND(((COUNT(*) * 1.0 / ((SELECT COUNT(*) FROM peers) * 1.0)) * 100.0), 2)
                   FROM ((SELECT DISTINCT peer FROM checks WHERE task LIKE '%' || 'CPP' || '%')
                    EXCEPT
                   (SELECT DISTINCT peer FROM checks WHERE task LIKE '%' || 'SQL' || '%'))
                            AS t1), (SELECT ROUND(((COUNT(*) * 1.0 / ((SELECT COUNT(*) FROM peers) * 1.0)) * 100.0), 2)
                                       FROM ((SELECT DISTINCT peer FROM checks WHERE task LIKE '%' || 'SQL' || '%')
                                        EXCEPT
                                       (SELECT DISTINCT peer FROM checks WHERE task LIKE '%' || 'CPP' || '%'))
                                                AS t1),
                (SELECT ROUND(((COUNT(*) * 1.0 / ((SELECT COUNT(*) FROM peers) * 1.0)) * 100.0), 2)
                   FROM ((SELECT DISTINCT checks.peer
                            FROM checks
                           WHERE checks.task LIKE '%' || 'CPP' || '%') AS t1
                       INNER JOIN (SELECT DISTINCT peer FROM checks WHERE task LIKE '%' || 'SQL' || '%') AS t2
                         ON t1.peer = t2.peer) AS res_table),
                (SELECT ROUND(((COUNT(*) * 1.0 / ((SELECT COUNT(*) FROM peers) * 1.0)) * 100.0), 2)
                   FROM checks
                            FULL JOIN peers p ON checks.peer = p.nickname
                  WHERE peer IS NULL
                     OR nickname IS NULL));
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL the_percentage_of_peers('cursor_11', 'SQL', 'CPP');
FETCH ALL IN "cursor_11";
COMMIT;




-- №12
DROP PROCEDURE IF EXISTS greatest_number_of_friends(IN ref refcursor, N int);

CREATE PROCEDURE greatest_number_of_friends(IN ref refcursor, N int)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT Peer, SUM(count) sum_count
          FROM ((SELECT DISTINCT t1.peer1 Peer, COUNT(*) count
                   FROM ((SELECT peer1, peer2 FROM friends) UNION ALL (SELECT peer2, peer1 FROM friends)) AS t1
                  GROUP BY t1.peer1)
           UNION
          (SELECT nickname, 0
             FROM peers)) AS t3
         GROUP BY Peer
         ORDER BY sum_count DESC
         LIMIT N;
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL greatest_number_of_friends('cursor_12', 3);
FETCH ALL IN "cursor_12";
COMMIT;




-- №13
DROP PROCEDURE IF EXISTS percentage_of_successes_on_birthday(IN ref refcursor);

CREATE PROCEDURE percentage_of_successes_on_birthday(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
        VALUES ((SELECT ROUND((COUNT(*) * 1.0 / (SELECT COUNT(*)
                                                   FROM (SELECT nickname, birthday, c.date
                                                           FROM peers
                                                                    JOIN checks c
                                                                         ON peers.nickname = c.peer AND
                                                                            EXTRACT(MONTH FROM birthday) =
                                                                            EXTRACT(MONTH FROM c.date) AND
                                                                            EXTRACT(DAY FROM birthday) =
                                                                            EXTRACT(DAY FROM c.date)) AS count_person) *
                               1.0) * 100.0, 2)
                   FROM (SELECT nickname, birthday, c.date, state
                           FROM peers
                                    JOIN checks c ON peers.nickname = c.peer AND
                                                     EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM c.date) AND
                                                     EXTRACT(DAY FROM birthday) = EXTRACT(DAY FROM c.date)
                                    JOIN p2p p ON c.id = p.checkid
                          WHERE state = 'Success') AS success_peer), (SELECT ROUND((COUNT(*) * 1.0 / (SELECT COUNT(*)
                                                                                                        FROM (SELECT nickname, birthday, c.date
                                                                                                                FROM peers
                                                                                                                         JOIN checks c
                                                                                                                              ON peers.nickname =
                                                                                                                                 c.peer AND
                                                                                                                                 EXTRACT(MONTH FROM birthday) =
                                                                                                                                 EXTRACT(MONTH FROM c.date) AND
                                                                                                                                 EXTRACT(DAY FROM birthday) =
                                                                                                                                 EXTRACT(DAY FROM c.date)) AS count_person) *
                                                                                    1.0) * 100.0, 2)
                                                                        FROM (SELECT nickname, birthday, c.date, state
                                                                                FROM peers
                                                                                         JOIN checks c
                                                                                              ON peers.nickname =
                                                                                                 c.peer AND
                                                                                                 EXTRACT(MONTH FROM birthday) =
                                                                                                 EXTRACT(MONTH FROM c.date) AND
                                                                                                 EXTRACT(DAY FROM birthday) =
                                                                                                 EXTRACT(DAY FROM c.date)
                                                                                         JOIN p2p p ON c.id = p.checkid
                                                                               WHERE state = 'Failure') AS success_peer));
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL percentage_of_successes_on_birthday('cursor_13');
FETCH ALL IN "cursor_13";
COMMIT;



-- №14
SELECT InsertIntoXP(11, 530);
SELECT InsertIntoXP(12, 600);
SELECT InsertIntoXP(13, 730);
SELECT InsertIntoXP(14, 500);
CALL InsertIntoP2P('Bumbum', 'Vovan', 'SQL_Task_5', 'Start', '00:00:00');
CALL InsertIntoP2P('Bumbum', 'Vovan', 'SQL_Task_5', 'Success', '00:13:00');
UPDATE checks
   SET date = '2023-02-17'
 WHERE id = 15;
SELECT InsertIntoXP(15, 900);


DROP PROCEDURE IF EXISTS total_amount_of_XP(IN ref refcursor);

CREATE PROCEDURE total_amount_of_XP(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer Peer, SUM(XP) XP
          FROM ((SELECT peer, SUM(xpamount) XP
                   FROM (SELECT c.peer peer, xpamount, task
                           FROM xp
                                    JOIN checks c ON c.id = xp.checkid
                                    JOIN p2p p ON c.id = p.checkid
                          WHERE state = 'Success') AS res
                  WHERE xpamount = (SELECT MAX(xpamount)
                                      FROM (SELECT c.peer, xpamount, task
                                              FROM xp
                                                       JOIN checks c ON c.id = xp.checkid
                                                       JOIN p2p p ON c.id = p.checkid
                                             WHERE state = 'Success') AS res2
                                     WHERE res2.peer = res.peer
                                       AND res2.task = res.task)
                  GROUP BY peer)
           UNION
          (SELECT nickname, 0 FROM peers)) AS res
         GROUP BY peer
         ORDER BY XP DESC;
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL total_amount_of_XP('cursor_14');
FETCH ALL IN "cursor_14";
COMMIT;




-- №15
DROP PROCEDURE IF EXISTS peers_who_did_the_given_tasks(IN ref refcursor, task_1 varchar, task_2 varchar, task_3 varchar);

CREATE PROCEDURE peers_who_did_the_given_tasks(IN ref refcursor, task_1 varchar, task_2 varchar, task_3 varchar)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT task_1_peer
          FROM (SELECT *
                  FROM (SELECT DISTINCT Peer task_1_peer
                          FROM checks
                                   JOIN p2p p ON checks.id = p.checkid
                         WHERE state = 'Success'
                           AND task = task_1) AS res1
                           LEFT JOIN
                       (SELECT DISTINCT Peer task_2_peer
                          FROM checks
                                   JOIN p2p p ON checks.id = p.checkid
                         WHERE state = 'Success'
                           AND task = task_2) AS res2 ON res1.task_1_peer = res2.task_2_peer
                 WHERE task_2_peer IS NOT NULL) AS res
        EXCEPT
        (SELECT DISTINCT nickname
           FROM peers
                    LEFT JOIN checks c ON peers.nickname = c.peer
                    LEFT JOIN p2p p ON c.id = p.checkid
          WHERE state = 'Success'
            AND task = task_3);
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL peers_who_did_the_given_tasks('cursor_15', 'CPP_Task_0', 'CPP_Task_1', 'CPP_Task_2');
FETCH ALL IN "cursor_15";
COMMIT;




-- №16
INSERT INTO tasks
VALUES ('kuku1', 'null', 222);
INSERT INTO tasks
VALUES ('kuku2', 'kuku1', 444);
INSERT INTO tasks
VALUES ('kuku3', 'kuku2', 666);

DROP PROCEDURE IF EXISTS recursive_common_table(IN ref refcursor);

CREATE PROCEDURE recursive_common_table(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
          WITH RECURSIVE r (title, parenttask, count) AS (SELECT title, parenttask, 0
                                                            FROM tasks
                                                           WHERE parenttask = 'null'
                                                           UNION
                                                          SELECT (SELECT t1.title FROM tasks t1 WHERE r.title = t1.parenttask),
                                                                 r.title,
                                                                 count + 1
                                                            FROM r
                                                           WHERE (SELECT t1.title FROM tasks t1 WHERE r.title = t1.parenttask) != 'null')

        SELECT title, count
          FROM r;
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL recursive_common_table('cursor_16');
FETCH ALL IN "cursor_16";
COMMIT;

DELETE
  FROM tasks
 WHERE title = 'kuku1';
DELETE
  FROM tasks
 WHERE title = 'kuku2';
DELETE
  FROM tasks
 WHERE title = 'kuku3';




-- №17
DROP PROCEDURE IF EXISTS lucky_days_for_checks(IN ref refcursor, N int);

CREATE PROCEDURE lucky_days_for_checks(IN ref refcursor, N int)
AS
$$
BEGIN
    OPEN ref FOR
        SELECT DISTINCT date
          FROM (SELECT state,
                       time,
                       date,
                       xpamount,
                       maxxp,
                       ROW_NUMBER() OVER (PARTITION BY date, state ORDER BY time) AS count_rew
                  FROM p2p
                           JOIN checks c ON c.id = p2p.checkid
                           LEFT JOIN xp x ON c.id = x.checkid
                           LEFT JOIN tasks t ON t.title = c.task
                 WHERE (((xpamount * 1.0 / maxxp * 1.0) * 100.0 > 80.0) OR xpamount ISNULL)
                   AND state != 'Start'
                 ORDER BY date, time) AS res
         WHERE state = 'Success'
           AND count_rew >= 1;
END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL lucky_days_for_checks('cursor_17', 2);
FETCH ALL IN "cursor_17";
COMMIT;




-- №18
DROP PROCEDURE IF EXISTS largest_number_of_completed_tasks(IN ref refcursor);

CREATE OR REPLACE PROCEDURE largest_number_of_completed_tasks(IN ref refcursor)
AS
$$
BEGIN
        OPEN ref FOR
    SELECT peer, COUNT(*)
    FROM checks
     JOIN p2p p ON checks.id = p.checkid
     JOIN verter v ON checks.id = v.checkid
     WHERE p.state = 'Success' AND v.state = 'Success'
    GROUP BY peer HAVING COUNT(*) >= ALL
    (SELECT COUNT(*)
    FROM checks
     JOIN p2p p ON checks.id = p.checkid
     JOIN verter v ON checks.id = v.checkid
     WHERE p.state = 'Success' AND v.state = 'Success'
    GROUP BY peer);

END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL largest_number_of_completed_tasks('cursor_name');
FETCH ALL IN "cursor_name";
COMMIT;




-- №19
DROP PROCEDURE IF EXISTS largest_number_of_XP(IN ref refcursor);

CREATE OR REPLACE PROCEDURE largest_number_of_XP(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
    SELECT peer, SUM(xpamount) XP
    FROM checks
    JOIN xp x ON checks.id = x.checkid
    GROUP BY peer HAVING SUM(xpamount) >= ALL
    (SELECT SUM(xpamount)
    FROM checks
    JOIN xp x ON checks.id = x.checkid
    GROUP BY peer);

END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL largest_number_of_XP('cursor_name');
FETCH ALL IN "cursor_name";
COMMIT;




-- №20
DROP PROCEDURE IF EXISTS largest_number_of_time(IN ref refcursor);

CREATE OR REPLACE PROCEDURE largest_number_of_time(IN ref refcursor)
AS
$$
BEGIN
    OPEN ref FOR
    WITH came_time AS(
    SELECT id,peer,time
    FROM TimeTracking
    WHERE state = '1'),
    gone_time AS(
    SELECT id,peer,time
    FROM TimeTracking
    WHERE state = '2'),
    JOINtable AS (
    SELECT DISTINCT ON (c.id) c.id AS id1, c.peer AS peer1, c.time AS time1, g.id AS id2, g.peer AS peer2, g.time AS time2
    FROM came_time c
    INNER JOIN gone_time g ON c.peer = g.peer AND c.time < g.time
    ORDER BY 1,2,3,6),

    peerandmaxtime AS (
    SELECT peer1 AS peer
    FROM JOINtable
    GROUP BY peer1
    HAVING (sum(time2 - time1)::time) = (SELECT (sum(time2 - time1)::time) AS time3
                                        FROM JOINtable
                                        GROUP BY peer1
                                        ORDER BY 1 DESC LIMIT 1)
    )
    SELECT * FROM peerandmaxtime;

END;
$$ LANGUAGE 'plpgsql';

BEGIN;
CALL largest_number_of_time('cursor_name');
FETCH ALL IN "cursor_name";
COMMIT;




-- №21
INSERT INTO timetracking (peer, date, time, state)
VALUES ('Leon', now()::date - 2, '12:37:00', 1);

DROP PROCEDURE IF EXISTS who_came_first(IN ref refcursor, M time, N integer);

CREATE OR REPLACE PROCEDURE who_came_first(IN ref refcursor, M time, N integer) AS
$$
BEGIN
    OPEN ref FOR
SELECT res.peer
FROM (SELECT peer, time
FROM timetracking
WHERE state = '1' AND time < M
GROUP BY 1,2) AS res
GROUP BY 1
HAVING count(*) > N;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL who_came_first('cursor_name', '15:00:00', 1);
FETCH ALL IN "cursor_name";
COMMIT;




-- №22
INSERT INTO timetracking (peer, date, time, state)
VALUES ('Doshirak', now()::date - 2, '11:37:00', 1),
       ('Doshirak', now()::date - 2, '19:37:00', 2),
       ('Mzoraida', now()::date - 3, '10:37:00', 1),
       ('Mzoraida', now()::date - 3, '15:37:00', 2),
       ('Mzoraida', now()::date - 3, '16:37:00', 1),
       ('Mzoraida', now()::date - 3, '21:37:00', 2),
       ('Mzoraida', now()::date - 4, '06:37:00', 1),
       ('Mzoraida', now()::date - 4, '21:37:00', 2),
       ('Vovan', now()::date - 2, '09:37:00', 1),
       ('Vovan', now()::date - 2, '19:37:00', 2),
       ('Vovan', now()::date - 3, '14:37:00', 1),
       ('Vovan', now()::date - 3, '21:37:00', 2),
       ('Vovan', now()::date - 4, '18:37:00', 1),
       ('Vovan', now()::date - 4, '21:37:00', 2);

CREATE OR REPLACE PROCEDURE who_left_campus(IN ref refcursor, N integer, M integer) AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer
        FROM (SELECT peer, date
                   , count(*) AS count_
              FROM timetracking
              WHERE state = '2'
                AND date >= (now()::date - N)
              GROUP BY 1, 2
              ORDER BY 2) AS res
        GROUP BY peer
        HAVING sum(count_) > M;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL who_left_campus('cursor_name', 3, 1);
FETCH ALL IN "cursor_name";
COMMIT;




-- №23
INSERT INTO timetracking (peer, date, time, state)
VALUES ('Doshirak', current_date, '11:37:00', 1),
       ('Doshirak', current_date, '19:37:00', 2),
       ('Mzoraida', current_date, '13:37:00', 1),
       ('Mzoraida', current_date, '21:37:00', 2),
       ('Bumbum', current_date, '06:37:00', 1),
       ('Bumbum', current_date, '21:37:00', 2),
       ('Leon', current_date, '09:37:00', 1),
       ('Leon', current_date, '19:37:00', 2),
       ('Vovan', current_date, '14:37:00', 1),
       ('Vovan', current_date, '21:37:00', 2),
       ('Sivana', current_date, '18:37:00', 1),
       ('Sivana', current_date, '21:37:00', 2);

CREATE OR REPLACE PROCEDURE peer_last_come(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        SELECT peer
        FROM (SELECT peer
              from timetracking
              where state = 1
                and date = current_date
              order by time desc
              limit 1) AS res;
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL peer_last_come('cursor_name');
FETCH ALL IN "cursor_name";
COMMIT;




-- №24
INSERT INTO timetracking (peer, date, time, state)
VALUES ('Bumbum', current_date - 1, '11:37:00', 1),
       ('Bumbum', current_date - 1, '12:37:00', 2),
       ('Bumbum', current_date - 1, '12:57:00', 1),
       ('Bumbum', current_date - 1, '20:37:00', 2),

       ('Leon', current_date - 1, '07:37:00', 1),
       ('Leon', current_date - 1, '09:37:00', 2),
       ('Leon', current_date - 1, '12:37:00', 1),
       ('Leon', current_date - 1, '16:37:00', 2),

       ('Doshirak', current_date - 1, '18:37:00', 1),
       ('Doshirak', current_date - 1, '20:37:00', 2),
       ('Doshirak', current_date - 1, '21:39:00', 1),
       ('Doshirak', current_date - 1, '21:40:00', 2);

CREATE OR REPLACE PROCEDURE left_campus_yesterday(IN ref refcursor, N integer) AS
$$
BEGIN
    OPEN ref FOR
        WITH in_ AS (SELECT id, peer, "date", "time"
                     FROM timetracking tt
                     WHERE tt.state = '1'
                       AND "date" = current_date - 1
                       AND NOT tt.time = (SELECT min("time")
                                          FROM timetracking tt2
                                          WHERE tt2.date = tt.date
                                            AND tt2.peer = tt.peer)
                     ORDER BY 2, 4),
             out_ AS
                 (SELECT id, peer, "date", "time"
                  FROM timetracking tt
                  WHERE tt.state = '2'
                    AND "date" = current_date - 1
                    AND NOT tt.time = (SELECT max("time")
                                       FROM timetracking tt2
                                       WHERE tt2.date = tt.date
                                         AND tt2.peer = tt.peer)
                  ORDER BY 2, 4),
             InAndOut AS
                 (SELECT DISTINCT ON (in_.id) in_.id, in_.peer, out_.time AS out_, in_.time AS in_
                  FROM in_
                           JOIN out_ ON in_.peer = out_.peer AND in_.date = out_.date
                  WHERE out_.time < in_.time)

        SELECT peer
        FROM InAndOut
        GROUP BY peer
        HAVING sum(InAndOut.in_ - InAndOut.out_) > make_time(N / 60, N - N / 60 * 60, 0.);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL left_campus_yesterday('cursor_name', 19);
FETCH ALL IN "cursor_name";
COMMIT;




-- №25
INSERT INTO timetracking (peer, date, time, state)
VALUES ('Doshirak', '2022-03-22', '11:37:00', 1),
       ('Doshirak', '2022-03-22', '19:37:00', 2),
       ('Doshirak', '2022-03-23', '13:37:00', 1),
       ('Doshirak', '2022-03-23', '21:37:00', 2),
       ('Doshirak', '2022-03-25', '13:37:00', 1),
       ('Doshirak', '2022-03-25', '21:37:00', 2),

       ('Mzoraida', '2022-02-22', '11:37:00', 1),
       ('Mzoraida', '2022-02-22', '19:37:00', 2),
       ('Mzoraida', '2022-02-23', '13:37:00', 1),
       ('Mzoraida', '2022-02-23', '21:37:00', 2),
       ('Mzoraida', '2022-02-25', '13:37:00', 1),
       ('Mzoraida', '2022-02-25', '21:37:00', 2),


       ('Bumbum', '2022-05-20', '10:37:00', 1),
       ('Bumbum', '2022-05-20', '20:37:00', 2),
       ('Bumbum', '2022-05-27', '11:37:00', 1),
       ('Bumbum', '2022-05-27', '21:37:00', 2),
       ('Bumbum', '2022-05-29', '09:37:00', 1),
       ('Bumbum', '2022-05-29', '21:37:00', 2),

       ('Leon', '2022-06-20', '10:37:00', 1),
       ('Leon', '2022-06-20', '20:37:00', 2),
       ('Leon', '2022-06-27', '11:37:00', 1),
       ('Leon', '2022-06-27', '21:37:00', 2),
       ('Leon', '2022-06-29', '19:37:00', 1),
       ('Leon', '2022-06-29', '21:37:00', 2),

       ('Vovan', '2022-09-20', '14:37:00', 1),
       ('Vovan', '2022-09-20', '20:37:00', 2),
       ('Vovan', '2022-09-27', '11:37:00', 1),
       ('Vovan', '2022-09-27', '21:37:00', 2),
       ('Vovan', '2022-09-29', '19:37:00', 1),
       ('Vovan', '2022-09-29', '21:37:00', 2),

       ('Sivana', '2022-10-20', '11:37:00', 1),
       ('Sivana', '2022-10-20', '20:37:00', 2),
       ('Sivana', '2022-10-27', '10:37:00', 1),
       ('Sivana', '2022-10-27', '21:37:00', 2),
       ('Sivana', '2022-10-29', '19:37:00', 1),
       ('Sivana', '2022-10-29', '21:37:00', 2);

CREATE OR REPLACE PROCEDURE the_percentage_of_early_entries(IN ref refcursor) AS
$$
BEGIN
    OPEN ref FOR
        WITH total_number_of_entries AS (SELECT p.birthday as month, count(*) total
                                         from peers as p
                                                  join timetracking t
                                                       on p.nickname = t.peer
                                         where state = 1
                                           and TO_CHAR(p.birthday, 'Month') = TO_CHAR(t.date, 'Month')
                                         group by nickname),
             number_of_early_entries AS (SELECT p.birthday as month, count(*) early
                                         from peers as p
                                                  join timetracking t
                                                       on p.nickname = t.peer
                                         where state = 1
                                           and TO_CHAR(p.birthday, 'Month') = TO_CHAR(t.date, 'Month')
                                           and extract(hour from t.time) < 12
                                         group by nickname)

        select TO_CHAR(e.month, 'Month') as Month, ((e.early::numeric / t.total) * 100)::int EarlyEntries
        from number_of_early_entries as e
                 join total_number_of_entries as t
                      on e.month = t.month
        order by extract(month from e.month);
END;
$$ LANGUAGE plpgsql;

BEGIN;
CALL the_percentage_of_early_entries('cursor_name');
FETCH ALL IN "cursor_name";
COMMIT;
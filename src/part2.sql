-- Удаление функций и триггеров
DROP PROCEDURE IF EXISTS InsertIntoP2P(Into_CheckedPeer varchar, Into_CheckingPeer varchar, Into_NameTask varchar,
                                      Into_State varchar, Into_Time time);
DROP PROCEDURE IF EXISTS InsertIntoVerter(Into_CheckingPeer varchar, Into_NameTask varchar, Into_State varchar,
                                         Into_Time time);
DROP TRIGGER IF EXISTS points_update ON p2p;
DROP FUNCTION IF EXISTS TriggerIntoP2P();
DROP FUNCTION IF EXISTS InsertIntoXP(Into_CheckID bigint, Into_XPAmount integer);
DROP TRIGGER IF EXISTS correct_xp ON xp;
DROP FUNCTION IF EXISTS TriggerIntoXP();


-- P2P функция
CREATE PROCEDURE InsertIntoP2P(Into_CheckedPeer varchar, Into_CheckingPeer varchar, Into_NameTask varchar,
                              Into_State varchar, Into_Time time)
AS
$$
BEGIN
    IF ((Into_State = 'Success' OR Into_State = 'Failure')
        AND Into_Time > (SELECT DISTINCT time
                           FROM p2p
                                    JOIN checks c ON c.id = p2p.checkid
                          WHERE checkingpeer = Into_CheckingPeer
                            AND state = 'Start'
                            AND c.date = (SELECT CURRENT_DATE)
                            AND checkid =
                                (SELECT MAX(checkid)
                                   FROM p2p
                                  WHERE checkingpeer = Into_CheckingPeer
                                    AND state = 'Start'))
        ) THEN
        INSERT INTO P2P (CheckID, CheckingPeer, State, Time)
        VALUES ((SELECT MAX(id) FROM checks WHERE peer = Into_CheckedPeer AND task = Into_NameTask), Into_CheckingPeer,
                Into_State, Into_Time);
    ELSEIF (Into_State = 'Start') THEN
        IF ((EXISTS(SELECT *
                      FROM p2p
                     WHERE checkid = (SELECT MAX(checkid)
                                        FROM (SELECT checkid
                                                FROM p2p
                                               WHERE checkingpeer = Into_CheckingPeer
                                                 AND state = 'Start'
                                               GROUP BY checkid) countp2p)
                       AND checkingpeer = Into_CheckingPeer
                       AND (state = 'Success' OR state = 'Failure'))) OR (NOT EXISTS(SELECT checkid
                                                                                       FROM p2p
                                                                                      WHERE state = 'Start'
                                                                                        AND checkingpeer = Into_CheckingPeer)))
        THEN
            INSERT INTO checks (peer, task, date)
            VALUES (Into_CheckedPeer, Into_NameTask, (SELECT CURRENT_DATE));
            INSERT INTO P2P (CheckID, CheckingPeer, State, Time)
            VALUES ((SELECT MAX(id) FROM checks WHERE peer = Into_CheckedPeer), Into_CheckingPeer, Into_State,
                    Into_Time);
        ELSE
            INSERT INTO P2P (CheckID, CheckingPeer, State, Time)
            VALUES ((SELECT DISTINCT MAX(checkid) FROM p2p WHERE checkingpeer = Into_CheckingPeer), Into_CheckingPeer,
                    Into_State,
                    (SELECT DISTINCT time
                       FROM p2p
                      WHERE checkingpeer = Into_CheckingPeer
                        AND checkid = (SELECT MAX(checkid) FROM p2p WHERE checkingpeer = Into_CheckingPeer)));
        END IF;
    END IF;
END;
$$ LANGUAGE 'plpgsql';

-- Verter функция
CREATE PROCEDURE InsertIntoVerter(Into_CheckingPeer varchar, Into_NameTask varchar, Into_State varchar,
                                 Into_Time time)
AS
$$
DECLARE
    check_ bigint = (SELECT checkid
                       FROM checks
                                JOIN p2p p ON checks.id = p.checkid
                      WHERE task = Into_NameTask
                        AND peer = Into_CheckingPeer
                        AND state = 'Success'
                      ORDER BY time DESC
                      LIMIT 1);
BEGIN
    IF (check_ IS NOT NULL) THEN
        IF (NOT EXISTS(SELECT checkid FROM verter WHERE checkid = check_ AND state = Into_State))
        THEN
            INSERT INTO Verter (CheckID, State, Time)
            VALUES (check_, Into_State, Into_Time);
        END IF;
    END IF;
END;
$$
    LANGUAGE 'plpgsql';


-- Триггерная функция для P2P
CREATE FUNCTION TriggerIntoP2P() RETURNS TRIGGER
AS
$$
DECLARE
    state_         varchar = (SELECT state
                                FROM p2p
                                         JOIN checks c ON c.id = p2p.checkid
                               ORDER BY p2p.id DESC
                               LIMIT 1);
    checking_peer_ varchar = (SELECT checkingpeer
                                FROM p2p
                                         JOIN checks c ON c.id = p2p.checkid
                               ORDER BY p2p.id DESC
                               LIMIT 1);
    checked_peer_  varchar = (SELECT peer checkedpeer
                                FROM p2p
                                         JOIN checks c ON c.id = p2p.checkid
                               ORDER BY p2p.id DESC
                               LIMIT 1);
BEGIN
    IF (state_ = 'Start')
    THEN
        IF ((SELECT COUNT(*) AS count
               FROM p2p
              WHERE checkid = new.checkid
                AND checkingpeer = new.checkingpeer
                AND time = new.time
                AND state = 'Start') < 2) THEN
            IF (NOT EXISTS(SELECT *
                             FROM transferredpoints
                            WHERE checkingpeer = checking_peer_
                              AND checkedpeer = checked_peer_))
            THEN
                INSERT INTO transferredpoints (checkingpeer, checkedpeer, pointsamount)
                VALUES (checking_peer_, checked_peer_, 1);
            ELSE
                UPDATE transferredpoints
                   SET pointsamount = (pointsamount + 1)
                 WHERE checkingpeer = checking_peer_
                   AND checkedpeer = checked_peer_;
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$
    LANGUAGE 'plpgsql';
END;


-- Триггер для P2P
CREATE TRIGGER points_update
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE TriggerIntoP2P();


-- XP функция
CREATE FUNCTION InsertIntoXP(Into_CheckID bigint, Into_XPAmount integer) RETURNS void AS
$$
BEGIN
    INSERT INTO xp (checkid, xpamount)
    VALUES (Into_CheckID, Into_XPAmount);
END;
$$
    LANGUAGE 'plpgsql';


--
CREATE FUNCTION TriggerIntoXP() RETURNS TRIGGER AS
$$
BEGIN
    IF (new.xpamount > 0 AND new.xpamount <= (SELECT maxxp
                                                FROM tasks
                                                         JOIN checks c ON tasks.title = c.task
                                               WHERE c.id = new.checkid) AND (EXISTS(SELECT state
                                                                                       FROM p2p
                                                                                                JOIN checks c2 ON c2.id = p2p.checkid
                                                                                      WHERE c2.id = new.checkid
                                                                                        AND state = 'Success')))
    THEN
        RETURN NEW;
    END IF;
    RETURN old;
END;
$$
    LANGUAGE 'plpgsql';


CREATE TRIGGER correct_xp
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE PROCEDURE TriggerIntoXP();


-- Into P2P
CALL InsertIntoP2P('Vovan', 'Sivana', 'CPP_Task_1', 'Start', '10:00');
-- Не добавит следующую запись а сошлется на предыдущую, так как у Sivana уже идет проверка
CALL InsertIntoP2P('Leon', 'Sivana', 'CPP_Task_1', 'Start', '10:07');
CALL InsertIntoP2P('Vovan', 'Sivana', 'CPP_Task_1', 'Success', '10:10');
CALL InsertIntoP2P('Mzoraida', 'Vovan', 'CPP_Task_1', 'Start', '9:00');
CALL InsertIntoP2P('Mzoraida', 'Vovan', 'CPP_Task_1', 'Success', '9:05');
CALL InsertIntoP2P('Mzoraida', 'Vovan', 'CPP_Task_2', 'Start', '9:10');
CALL InsertIntoP2P('Mzoraida', 'Vovan', 'CPP_Task_2', 'Success', '9:15');
CALL InsertIntoP2P('Sivana', 'Bumbum', 'CPP_Task_1', 'Start', '11:00');
CALL InsertIntoP2P('Sivana', 'Bumbum', 'CPP_Task_1', 'Failure', '11:35');
CALL InsertIntoP2P('Sivana', 'Bumbum', 'CPP_Task_1', 'Start', '12:00');
CALL InsertIntoP2P('Sivana', 'Bumbum', 'CPP_Task_1', 'Success', '12:15');

-- Into Verter
CALL InsertIntoVerter('Vovan', 'CPP_Task_1', 'Start', '12:59');
CALL InsertIntoVerter('Vovan', 'CPP_Task_1', 'Success', '13:00');
CALL InsertIntoVerter('Mzoraida', 'CPP_Task_1', 'Start', '13:59');
CALL InsertIntoVerter('Mzoraida', 'CPP_Task_1', 'Success', '14:00');
CALL InsertIntoVerter('Mzoraida', 'CPP_Task_2', 'Start', '13:59');
CALL InsertIntoVerter('Mzoraida', 'CPP_Task_2', 'Success', '14:00');
CALL InsertIntoVerter('Sivana', 'CPP_Task_1', 'Start', '13:59');
CALL InsertIntoVerter('Sivana', 'CPP_Task_1', 'Success', '14:00');

-- Into XP
-- Не добавит
SELECT InsertIntoXP(6, 550);
-- Добавит
SELECT InsertIntoXP(6, 500);
-- Не добавит
SELECT InsertIntoXP(7, -500);
-- Добавит
SELECT InsertIntoXP(7, 450);
SELECT InsertIntoXP(8, 500);
SELECT InsertIntoXP(10, 380);

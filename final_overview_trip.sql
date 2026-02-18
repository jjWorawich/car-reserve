WITH RECURSIVE date_series AS (
    SELECT 
        MIN(CAST(vq.date_queue AS DATE)) AS work_date
    FROM book_car.vehicle_queue vq

    UNION ALL

    SELECT 
        ds.work_date + INTERVAL 1 DAY
    FROM date_series ds
    WHERE ds.work_date < CURDATE() + INTERVAL 30 DAY
),

max_rounds AS (
    SELECT
        CAST(q.date_queue AS DATE) AS work_date,
        COALESCE(bk.branch_id, v.branch_id) AS branch_id,
        MAX(q.trip_round_id) AS max_trip_round_id
    FROM book_car.vehicle_queue q
    LEFT JOIN book_car.vehicle v 
        ON q.vehicle_id = v.vehicle_id
    LEFT JOIN book_car.booking bk 
        ON q.booking_tag = bk.booking_tag
    GROUP BY
        CAST(q.date_queue AS DATE),
        COALESCE(bk.branch_id, v.branch_id)

    UNION ALL

    SELECT
        CAST(bv.date AS DATE) AS work_date,
        bv.branch_id,
        MAX(bv.trip_round_id) AS max_trip_round_id
    FROM book_car.broken_vehicle bv
    GROUP BY
        CAST(bv.date AS DATE),
        bv.branch_id
),

combined_max AS (
    SELECT
        ds.work_date,
        b.branch_id,
        GREATEST(COALESCE(MAX(mr.max_trip_round_id), 4), 4) AS max_trip_round_id
    FROM date_series ds
    JOIN book_car.branch b
    LEFT JOIN max_rounds mr
        ON ds.work_date = mr.work_date
       AND b.branch_id = mr.branch_id
    GROUP BY
        ds.work_date,
        b.branch_id
),

rounds AS (
    SELECT
        work_date,
        branch_id,
        1 AS trip_round_id
    FROM combined_max

    UNION ALL

    SELECT
        r.work_date,
        r.branch_id,
        r.trip_round_id + 1
    FROM rounds r
    JOIN combined_max cm
        ON r.work_date = cm.work_date
       AND r.branch_id = cm.branch_id
    WHERE r.trip_round_id < cm.max_trip_round_id
),

vehicle_move_history AS (
    SELECT
        mv.vehicle_id,
        mv.to_branch_id,
        mv.movement_date
    FROM book_car.move_vehicle mv
),

vehicle_effective_branch AS (
    SELECT
        v.vehicle_id,
        ds.work_date,
        CASE 
            WHEN mv.vehicle_id IS NOT NULL 
                THEN mv.to_branch_id
            ELSE v.branch_id
        END AS effective_branch_id,
        v.vehicle_type
    FROM book_car.vehicle v
    JOIN date_series ds
    LEFT JOIN vehicle_move_history mv
        ON mv.vehicle_id = v.vehicle_id
       AND mv.movement_date = ds.work_date
),

skeleton AS (
    SELECT
        r.work_date,
        veb.vehicle_id,
        veb.effective_branch_id AS v_branch_id,
        veb.vehicle_type,
        r.trip_round_id,
        tr.name AS trip_name
    FROM vehicle_effective_branch veb
    JOIN rounds r
        ON veb.effective_branch_id = r.branch_id
       AND veb.work_date = r.work_date
    JOIN book_car.trip_round tr
        ON r.trip_round_id = tr.trip_round_id
),

queue_data AS (
    SELECT
        CAST(q.date_queue AS DATE) AS work_date,
        COALESCE(bk.branch_id, v.branch_id) AS branch_id,
        v.vehicle_id,
        v.vehicle_type,
        q.trip_round_id,
        q.queue_id,
        q.no_rev_id,
        q.status_id
    FROM book_car.vehicle_queue q
    JOIN book_car.vehicle v
        ON q.vehicle_id = v.vehicle_id
    LEFT JOIN book_car.booking bk
        ON q.booking_tag = bk.booking_tag
),

broken_data AS (
    SELECT DISTINCT
        CAST(bv.date AS DATE) AS work_date,
        bv.branch_id,
        bv.vehicle_id,
        v.vehicle_type,
        bv.trip_round_id,
        bv.queue_id,
        bv.no_rev_id,
        NULL AS status_id
    FROM book_car.broken_vehicle bv
    JOIN book_car.vehicle v
        ON bv.vehicle_id = v.vehicle_id
),

unioned AS (
    SELECT * FROM queue_data
    UNION ALL
    SELECT * FROM broken_data
),

classified AS (
    SELECT
        s.work_date,
        s.v_branch_id AS branch_id,
        b.branch_name,
        s.vehicle_id,
        s.vehicle_type,
        s.trip_round_id,
        s.trip_name,
        u.queue_id,
        u.no_rev_id,
        u.status_id,
        CASE
            WHEN u.queue_id IS NULL AND u.no_rev_id IS NULL 
                THEN 'remain'
            WHEN u.queue_id IS NOT NULL 
                 AND u.no_rev_id IS NULL 
                 AND s.trip_round_id <= 4 
                 AND u.status_id <> 2 
                THEN 'booked_1_4'
            WHEN u.queue_id IS NOT NULL 
                 AND u.no_rev_id IS NULL 
                 AND s.trip_round_id > 4 
                 AND u.status_id <> 2 
                THEN 'booked_5_plus'
            WHEN u.queue_id IS NOT NULL 
                 AND u.no_rev_id IS NOT NULL 
                THEN 'no_revenue'
            WHEN u.queue_id IS NULL 
                 AND u.no_rev_id IS NOT NULL 
                THEN 'no_revenue'
            WHEN u.queue_id IS NOT NULL 
                 AND u.status_id = 2 
                THEN 'no_revenue'
        END AS trip_status
    FROM skeleton s
    LEFT JOIN unioned u
        ON s.work_date = u.work_date
       AND s.v_branch_id = u.branch_id
       AND s.vehicle_id = u.vehicle_id
       AND s.trip_round_id = u.trip_round_id
    JOIN book_car.branch b
        ON s.v_branch_id = b.branch_id
),

statuses AS (
    SELECT 'booked' AS trip_status
    UNION ALL SELECT 'remain'
    UNION ALL SELECT 'no_revenue'
)

SELECT
    c.work_date,
    s.trip_status,
    c.branch_id,
    c.branch_name,
    c.vehicle_type,
    CASE
        WHEN s.trip_status = 'booked' THEN
            COUNT(
                CASE 
                    WHEN c.trip_status IN ('booked_1_4', 'booked_5_plus') 
                    THEN 1 
                END
            )
        ELSE
            COUNT(
                CASE 
                    WHEN c.trip_status = s.trip_status 
                    THEN 1 
                END
            )
    END AS trip_times
FROM classified c
JOIN statuses s
GROUP BY
    c.work_date,
    c.branch_id,
    c.branch_name,
    c.vehicle_type,
    s.trip_status
ORDER BY
    c.work_date,
    c.branch_id,
    c.vehicle_type,
    s.trip_status;

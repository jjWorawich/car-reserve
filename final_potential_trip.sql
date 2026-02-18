WITH RECURSIVE date_series AS (
    SELECT
        MIN(CAST(q.date_queue AS DATE)) AS work_date
    FROM book_car.vehicle_queue q

    UNION ALL

    SELECT
        ds.work_date + INTERVAL 1 DAY
    FROM date_series ds
    WHERE ds.work_date < CURDATE() + INTERVAL 30 DAY
),

max_rounds AS (
    SELECT
        CAST(q.date_queue AS DATE) AS work_date,
        v.branch_id,
        MAX(q.trip_round_id) AS max_trip_round_id
    FROM book_car.vehicle_queue q
    JOIN book_car.vehicle v
        ON q.vehicle_id = v.vehicle_id
    GROUP BY
        CAST(q.date_queue AS DATE),
        v.branch_id

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
        GREATEST(
            COALESCE(MAX(mr.max_trip_round_id), 4),
            4
        ) AS max_trip_round_id
    FROM date_series ds
    JOIN book_car.branch b
    LEFT JOIN max_rounds mr
        ON ds.work_date = mr.work_date
       AND b.branch_id = mr.branch_id
    GROUP BY
        ds.work_date,
        b.branch_id
),

vehicle_effective_branch AS (
    SELECT
        v.vehicle_id,
        ds.work_date,
        CASE
            WHEN mv.movement_date IS NOT NULL
             AND ds.work_date >= mv.movement_date
                THEN mv.to_branch_id
            ELSE v.branch_id
        END AS effective_branch_id,
        v.vehicle_type
    FROM book_car.vehicle v
    JOIN date_series ds
    LEFT JOIN book_car.move_vehicle mv
        ON v.vehicle_id = mv.vehicle_id
       AND mv.movement_date = ds.work_date
),

potential_base AS (
    SELECT
        eb.work_date,
        eb.effective_branch_id AS branch_id,
        eb.vehicle_type,
        COUNT(DISTINCT eb.vehicle_id) AS num_car,
        cm.max_trip_round_id,
        COUNT(DISTINCT eb.vehicle_id) * cm.max_trip_round_id AS potential_trip
    FROM vehicle_effective_branch eb
    JOIN combined_max cm
        ON eb.work_date = cm.work_date
       AND eb.effective_branch_id = cm.branch_id
    GROUP BY
        eb.work_date,
        eb.effective_branch_id,
        eb.vehicle_type,
        cm.max_trip_round_id
)

SELECT
    pb.work_date,
    pb.vehicle_type,
    SUM(pb.potential_trip) AS potential_trip
FROM potential_base pb
JOIN book_car.branch b
    ON pb.branch_id = b.branch_id
GROUP BY
    pb.work_date,
    pb.vehicle_type
ORDER BY
    pb.work_date,
    pb.vehicle_type;

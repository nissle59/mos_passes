INSERT INTO
    passes.passes(
        series,
        "number",
        time_of_day,
        status,
        vin,
        reg_number,
        start_date,
        finish_date,
        updated_at
    )
    VALUES (
        {{SERIES}},
        {{NUMBER}},
        {{TD}},
        {{STATUS}},
        {{VIN}},
        {{REG}},
        {{START}},
        {{FINISH}},
        CURRENT_TIMESTAMP
    )
ON CONFLICT (series, "number") DO UPDATE SET
    time_of_day = {{TD}},
    status = {{STATUS}},
    vin = {{VIN}},
    reg_number = {{REG}},
    start_date = {{START}},
    finish_date = {{FINISH}},
    updated_at = CURRENT_TIMESTAMP
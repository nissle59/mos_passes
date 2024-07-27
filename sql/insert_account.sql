INSERT INTO
    passes.accounts(
        login,
        password,
        cookie_value,
        active
    )
    VALUES (
        {{USERNAME}},
        {{PASSWORD}},
        {{COOKIE}},
        true
    )
ON CONFLICT (login) DO UPDATE SET
    password = {{PASSWORD}},
    cookie_value = {{COOKIE}},
    active = true
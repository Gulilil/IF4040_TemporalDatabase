CREATE_TABLE_INVENTORY_DETAILS = """CREATE TABLE IF NOT EXISTS inventory_details (
    id SERIAL PRIMARY KEY,
    s_id VARCHAR(5) NOT NULL,
    p_id VARCHAR(5) NOT NULL,
    category VARCHAR(50) NOT NULL,
    region VARCHAR(10), 
    seasonality VARCHAR(10),
    vt_start TIMESTAMP NOT NULL,
    vt_end TIMESTAMP NOT NULL DEFAULT 'infinity',
    tt_start TIMESTAMP NOT NULL DEFAULT NOW(),
    tt_end TIMESTAMP NOT NULL DEFAULT 'infinity'
);
"""

CREATE_TABLE_INVENTORY_SALES = """CREATE TABLE IF NOT EXISTS inventory_sales (
    id SERIAL PRIMARY KEY, 
    s_id VARCHAR(5) NOT NULL,
    p_id VARCHAR(5) NOT NULL,
    amount INT NOT NULL DEFAULT 0,
    units_sold INT NOT NULL DEFAULT 0, 
    units_ordered INT NOT NULL DEFAULT 0,
    price FLOAT NOT NULL,
    discount INT NOT NULL DEFAULT 0,
    refer_id INT NOT NULL,
    vt_start TIMESTAMP NOT NULL,
    vt_end TIMESTAMP NOT NULL DEFAULT 'infinity',
    tt_start TIMESTAMP NOT NULL DEFAULT NOW(),
    tt_end TIMESTAMP NOT NULL DEFAULT 'infinity',
    FOREIGN KEY (refer_id) REFERENCES inventory_details(id) ON UPDATE CASCADE ON DELETE CASCADE
);
"""


import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# Function to establish connection to the database
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("POSTGRE_DB_NAME"),
        user=os.getenv("POSTGRE_DB_USER"),
        password=os.getenv("POSTGRE_DB_PASSWORD"),
        host=os.getenv("POSTGRE_DB_HOST"),
        port=os.getenv("POSTGRE_DB_PORT")
    )

def create_coalesce_category_function(connection):
    query = """
    CREATE OR REPLACE FUNCTION coalesce_category()
    RETURNS TABLE (
        s_id TEXT,
        p_id TEXT,
        category TEXT,
        vt_start TIMESTAMP,
        vt_end TIMESTAMP,
        tt_start TIMESTAMP,
        tt_end TIMESTAMP
    ) AS $$
    DECLARE
        row RECORD;
        inner_row RECORD;
        temp_table_name TEXT := 'temp_coalesce_category';
        current_vt_start TIMESTAMP;
        current_vt_end TIMESTAMP;
        current_tt_start TIMESTAMP;
        current_tt_end TIMESTAMP;
    BEGIN
        -- Create a temporary table to store the results
        EXECUTE FORMAT(
            'CREATE TEMP TABLE %I (
                s_id TEXT,
                p_id TEXT,
                category TEXT,
                vt_start TIMESTAMP,
                vt_end TIMESTAMP,
                tt_start TIMESTAMP,
                tt_end TIMESTAMP
            )', temp_table_name
        );

        -- Iterate through the grouped data
        FOR row IN EXECUTE FORMAT(
            'SELECT DISTINCT s_id, p_id, category FROM inventory_details ORDER BY s_id, p_id, category'
        )
        LOOP
            -- Initialize start and end for the group
            current_vt_start := NULL;
            current_vt_end := NULL;
            current_tt_start := NULL;
            current_tt_end := NULL;

            FOR inner_row IN EXECUTE FORMAT(
                'SELECT s_id, p_id, category, vt_start, vt_end, tt_start, tt_end FROM inventory_details WHERE s_id = %L AND p_id = %L AND category = %L ORDER BY vt_start, tt_start',
                row.s_id, row.p_id, row.category
            )
            LOOP
                IF current_vt_start IS NULL THEN
                    -- Initialize the first interval
                    current_vt_start := inner_row.vt_start;
                    current_vt_end := inner_row.vt_end;
                    current_tt_start := inner_row.tt_start;
                    current_tt_end := inner_row.tt_end;
                ELSE
                    IF inner_row.vt_start <= current_vt_end THEN
                        -- Merge overlapping intervals
                        current_vt_end := GREATEST(current_vt_end, inner_row.vt_end);
                        current_tt_start := LEAST(current_tt_start, inner_row.tt_start);
                        current_tt_end := GREATEST(current_tt_end, inner_row.tt_end);
                    ELSE
                        -- Push the current interval and reset
                        EXECUTE FORMAT(
                            'INSERT INTO %I (s_id, p_id, category, vt_start, vt_end, tt_start, tt_end) VALUES (%L, %L, %L, %L, %L, %L, %L)',
                            temp_table_name, inner_row.s_id, inner_row.p_id, inner_row.category,
                            current_vt_start, current_vt_end, current_tt_start, current_tt_end
                        );
                        current_vt_start := inner_row.vt_start;
                        current_vt_end := inner_row.vt_end;
                        current_tt_start := inner_row.tt_start;
                        current_tt_end := inner_row.tt_end;
                    END IF;
                END IF;
            END LOOP;

            -- Add the last interval for the group
            IF current_vt_start IS NOT NULL THEN
                EXECUTE FORMAT(
                    'INSERT INTO %I (s_id, p_id, category, vt_start, vt_end, tt_start, tt_end) VALUES (%L, %L, %L, %L, %L, %L, %L)',
                    temp_table_name, row.s_id, row.p_id, row.category,
                    current_vt_start, current_vt_end, current_tt_start, current_tt_end
                );
            END IF;
        END LOOP;

        -- Return the results from the temporary table
        RETURN QUERY EXECUTE FORMAT('SELECT * FROM %I', temp_table_name);

        -- Drop the temporary table
        EXECUTE FORMAT('DROP TABLE %I', temp_table_name);
    END;
    $$ LANGUAGE plpgsql;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        connection.commit()
        print("Function `coalesce_category` created successfully.")

def create_coalesce_function(connection):
    query = """
        CREATE OR REPLACE FUNCTION coalesce_intervals(
            table_name TEXT
        ) RETURNS TABLE (
            s_id TEXT,
            p_id TEXT,
            vt_start TIMESTAMP,
            vt_end TIMESTAMP,
            tt_start TIMESTAMP,
            tt_end TIMESTAMP
        ) AS $$
        DECLARE
            row RECORD;
            inner_row RECORD;
            temp_table_name TEXT := 'temp_coalesce_intervals';
            current_vt_start TIMESTAMP;
            current_vt_end TIMESTAMP;
            current_tt_start TIMESTAMP;
            current_tt_end TIMESTAMP;
        BEGIN
            -- Create a temporary table to store the results
            EXECUTE FORMAT(
                'CREATE TEMP TABLE %I (
                    s_id TEXT,
                    p_id TEXT,
                    vt_start TIMESTAMP,
                    vt_end TIMESTAMP,
                    tt_start TIMESTAMP,
                    tt_end TIMESTAMP
                )', temp_table_name
            );

            -- Iterate through the grouped data
            FOR row IN EXECUTE FORMAT(
                'SELECT DISTINCT s_id, p_id FROM %I ORDER BY s_id, p_id',
                table_name
            )
            LOOP
                -- Initialize start and end for the group
                current_vt_start := NULL;
                current_vt_end := NULL;
                current_tt_start := NULL;
                current_tt_end := NULL;

                FOR inner_row IN EXECUTE FORMAT(
                    'SELECT s_id, p_id, vt_start, vt_end, tt_start, tt_end FROM %I WHERE s_id = %L AND p_id = %L ORDER BY vt_start, tt_start',
                    table_name, row.s_id, row.p_id
                )
                LOOP
                    IF current_vt_start IS NULL THEN
                        -- Initialize the first interval
                        current_vt_start := inner_row.vt_start;
                        current_vt_end := inner_row.vt_end;
                        current_tt_start := inner_row.tt_start;
                        current_tt_end := inner_row.tt_end;
                    ELSE
                        IF inner_row.vt_start <= current_vt_end THEN
                            -- Merge overlapping intervals
                            current_vt_end := GREATEST(current_vt_end, inner_row.vt_end);
                            current_tt_start := LEAST(current_tt_start, inner_row.tt_start);
                            current_tt_end := GREATEST(current_tt_end, inner_row.tt_end);
                        ELSE
                            -- Push the current interval and reset
                            EXECUTE FORMAT(
                                'INSERT INTO %I (s_id, p_id, vt_start, vt_end, tt_start, tt_end) VALUES (%L, %L, %L, %L, %L, %L)',
                                temp_table_name, inner_row.s_id, inner_row.p_id,
                                current_vt_start, current_vt_end, current_tt_start, current_tt_end
                            );
                            current_vt_start := inner_row.vt_start;
                            current_vt_end := inner_row.vt_end;
                            current_tt_start := inner_row.tt_start;
                            current_tt_end := inner_row.tt_end;
                        END IF;
                    END IF;
                END LOOP;

                -- Add the last interval for the group
                IF current_vt_start IS NOT NULL THEN
                    EXECUTE FORMAT(
                        'INSERT INTO %I (s_id, p_id, vt_start, vt_end, tt_start, tt_end) VALUES (%L, %L, %L, %L, %L, %L)',
                        temp_table_name, row.s_id, row.p_id,
                        current_vt_start, current_vt_end, current_tt_start, current_tt_end
                    );
                END IF;
            END LOOP;

            -- Return the results from the temporary table
            RETURN QUERY EXECUTE FORMAT('SELECT * FROM %I', temp_table_name);

            -- Drop the temporary table
            EXECUTE FORMAT('DROP TABLE %I', temp_table_name);
        END;
        $$ LANGUAGE plpgsql;
        """
    
    with connection.cursor() as cursor:
        cursor.execute(query)
        connection.commit()
        print("Function `coalesce_intervals` created successfully.")

def main():
    connection = get_connection()
    try:
        create_coalesce_category_function(connection)
        create_coalesce_function(connection)
    finally:
        connection.close()

if __name__ == "__main__":
    main()

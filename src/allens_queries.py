# Allens Interval Functions as SQL Strings

before_function = """
CREATE OR REPLACE FUNCTION before(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP, inverse BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
BEGIN
    IF inverse THEN
        RETURN start1 > end2; -- After
    ELSE
        RETURN end1 < start2; -- Before
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

meets_function = """
CREATE OR REPLACE FUNCTION meets(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP, inverse BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
BEGIN
    IF inverse THEN
        RETURN start1 = end2; -- Met by
    ELSE
        RETURN end1 = start2; -- Meets
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

overlaps_function = """
CREATE OR REPLACE FUNCTION overlaps(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP, inverse BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
BEGIN
    IF inverse THEN
        RETURN start2 < start1 AND end2 > start1 AND end2 < end1; -- Overlapped by
    ELSE
        RETURN start1 < start2 AND end1 > start2 AND end1 < end2; -- Overlaps
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

starts_function = """
CREATE OR REPLACE FUNCTION starts(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP, inverse BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
BEGIN
    IF inverse THEN
        RETURN start1 = start2 AND end1 > end2; -- Started by
    ELSE
        RETURN start1 = start2 AND end1 < end2; -- Starts
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

during_function = """
CREATE OR REPLACE FUNCTION during(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP, inverse BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
BEGIN
    IF inverse THEN
        RETURN start1 < start2 AND end1 > end2; -- Contains
    ELSE
        RETURN start1 > start2 AND end1 < end2; -- During
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

finishes_function = """
CREATE OR REPLACE FUNCTION finishes(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP, inverse BOOLEAN DEFAULT FALSE)
RETURNS BOOLEAN AS $$
BEGIN
    IF inverse THEN
        RETURN end1 = end2 AND start1 < start2; -- Finished by
    ELSE
        RETURN end1 = end2 AND start1 > start2; -- Finishes
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

equals_function = """
CREATE OR REPLACE FUNCTION equals(start1 TIMESTAMP, end1 TIMESTAMP, start2 TIMESTAMP, end2 TIMESTAMP)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN start1 = start2 AND end1 = end2;
END;
$$ LANGUAGE plpgsql;
"""

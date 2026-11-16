-- 0003: set_updated_at() stamps now() only when the UPDATE did not set
-- updated_at itself. Live updates are unchanged; history mode (M4) can
-- write a backdated updated_at, which Silver reads as valid time.

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at THEN
        NEW.updated_at := now();
    END IF;
    RETURN NEW;
END;
$$;
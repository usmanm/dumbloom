CREATE TYPE dumbloom AS (
  m    integer,
  k    integer,
  bits integer[]
);

CREATE FUNCTION dumbloom_empty (
  p float8 DEFAULT 0.01,
  n integer DEFAULT 10000
) RETURNS dumbloom AS
$$
DECLARE
  m    integer;
  k    integer;
  i    integer;
  bits integer[];   
BEGIN
  m := abs(ceil(n * ln(p) / (ln(2) ^ 2)))::integer;
  k := round(ln(2) * m / n)::integer;
  bits := NULL;

  FOR i in 1 .. ceil(m / 32.0) LOOP
    bits := array_append(bits, 0);
  END LOOP;

  RETURN (m, k, bits)::dumbloom;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION dumbloom_fingerprint (
  b    dumbloom,
  item text
) RETURNS integer[] AS 
$$
DECLARE
  h1          bigint;
  h2          bigint;
  i           integer;
  fingerprint integer[];
BEGIN
  h1 := abs(hashtext(upper(item)));
  h2 := abs(hashtext(lower(item)));
  fingerprint := NULL;

  FOR i IN 1 .. b.k LOOP
    fingerprint := array_append(fingerprint, ((h1 + i * h2) % b.m)::integer);
  END LOOP;

  RETURN fingerprint;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION dumbloom_add (
  b    dumbloom,
  item text,
  p    float8 DEFAULT 0.01,
  n    integer DEFAULT 10000
) RETURNS dumbloom AS 
$$
DECLARE
  i    integer;
  idx  integer;
BEGIN
  IF b IS NULL THEN
    b := dumbloom_empty(p, n);
  END IF;

  FOREACH i IN ARRAY dumbloom_fingerprint(b, item) LOOP
    idx := i / 32 + 1;
    b.bits[idx] := b.bits[idx] | (1 << (i % 32));
  END LOOP;

  RETURN b;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION dumbloom_add (
  b    dumbloom,
  item text
) RETURNS dumbloom AS 
$$
BEGIN
  RETURN dumbloom_add(b, item, 0.01, 10000);
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION dumbloom_contains (
  b    dumbloom,
  item text
) RETURNS boolean AS 
$$
DECLARE
  i   integer;
  idx integer;
BEGIN
  IF b IS NULL THEN
    RETURN FALSE;
  END IF;

  FOREACH i IN ARRAY dumbloom_fingerprint(b, item) LOOP
    idx := i / 32 + 1;
    IF NOT (b.bits[idx] & (1 << (i % 32)))::boolean THEN
      RETURN FALSE;
    END IF;
  END LOOP;

  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION dumbloom_union (
  b1 dumbloom,
  b2 dumbloom
) RETURNS dumbloom AS 
$$
DECLARE
  i    integer;
  bits integer[];
BEGIN
  IF b1 IS NULL THEN
    RETURN b2;
  END IF;

  IF NOT (b1.m = b2.m AND b1.k = b2.k) THEN
    RAISE EXCEPTION 'bloom filter mismatch!';
  END IF;

  bits := NULL;

  FOR i IN 1 .. ceil(b1.m / 32.0) LOOP
    bits := array_append(bits, b1.bits[i] | b2.bits[i]);
  END LOOP;

  RETURN (b1.m, b1.k, bits)::dumbloom;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE AGGREGATE dumbloom_agg (text) (
  stype = dumbloom,
  sfunc = dumbloom_add,
  combinefunc = dumbloom_union
);

CREATE AGGREGATE dumbloom_agg (text, float8, integer) (
  stype = dumbloom,
  sfunc = dumbloom_add,
  combinefunc = dumbloom_union
);

CREATE AGGREGATE dumbloom_union_agg (dumbloom) (
  stype = dumbloom,
  sfunc = dumbloom_union,
  combinefunc = dumbloom_union
);

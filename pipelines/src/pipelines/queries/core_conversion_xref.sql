-- DORMANT — do not run or import unless a core system conversion is in progress.
--
-- On a core conversion, populate this table to map old source_person_serials to
-- new ones via the stable person_key surrogate in person_identity.
-- After populating, update person_identity.source_person_serial
-- to the new source ID for each person_key.

CREATE TABLE IF NOT EXISTS /*check schema name here - may have changed*/ prod.person_source_xref (
    person_key              BIGINT NOT NULL REFERENCES prod.person_identity(person_key),
    source_system           TEXT NOT NULL,
    source_person_serial    TEXT NOT NULL,
    valid_from              DATE,
    valid_to                DATE,
    is_current              BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (source_system, source_person_serial),
    UNIQUE (person_key, source_system, source_person_serial)
);

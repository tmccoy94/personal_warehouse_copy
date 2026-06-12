import json
from decimal import Decimal, InvalidOperation

import dagster
from dagster import asset
from sqlalchemy import text

from pipelines.defs.assets.warehouse_loaders.ks_supplementals import ks_person_contact_table
from pipelines.defs.assets.warehouse_schema.seed_data import warehouse_schema_ready
from pipelines.defs.resources import HubspotApiResource, WarehousePgResource
from pipelines.loading import audit_load


def _match_emails_to_person_keys(engine, emails: list[str]) -> dict[str, int]:
    """Return {email: person_key} for emails that match exactly one keystone member.

    Emails with zero matches or multiple matches (ambiguous) are excluded — caller
    should leave person_key NULL for those contacts.
    """
    if not emails:
        return {}
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT contact_value, person_key "
                "FROM person_contact "
                "WHERE source_system = 'keystone' "
                "  AND contact_type = 'email' "
                "  AND contact_value = ANY(:emails)"
            ),
            {"emails": emails},
        )
        email_to_keys: dict[str, list[int]] = {}
        for row in result:
            email_to_keys.setdefault(row.contact_value, []).append(int(row.person_key))
    return {email: keys[0] for email, keys in email_to_keys.items() if len(keys) == 1}


def _parse_amount(amount_str: str | None) -> Decimal | None:
    """Cast a HubSpot amount string to Decimal — returns None for blank or malformed values."""
    if not amount_str:
        return None
    try:
        return Decimal(amount_str)
    except InvalidOperation:
        return None


@asset(group_name="hubspot_loaders", deps=[ks_person_contact_table])
def hs_contacts_table(
    context: dagster.AssetExecutionContext,
    hubspot_api: HubspotApiResource,
    warehouse_pg: WarehousePgResource,
):
    # TO_RESOLVE: confirm property names against live HubSpot sandbox before first run
    contacts_data = list(
        hubspot_api.paginate(
            "/crm/v3/objects/contacts",
            params={
                "properties": (
                    "email,firstname,lastname,phone,company,"
                    "lifecyclestage,createdate,hs_lastmodifieddate"
                )
            },
        )
    )

    engine = warehouse_pg.get_engine()

    # Batch email → person_key resolution (left-join semantics: NULL if no unique match)
    emails = [
        row["properties"].get("email")
        for row in contacts_data
        if row.get("properties", {}).get("email")
    ]
    email_to_person_key = _match_emails_to_person_keys(engine, emails)

    upserted = 0
    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="hubspot",
    ) as stats:
        with engine.begin() as pg_conn:
            for row in contacts_data:
                props = row.get("properties", {})
                hs_contact_id = row["id"]
                email = props.get("email")
                person_key = email_to_person_key.get(email) if email else None

                result = pg_conn.execute(
                    text("""
                        INSERT INTO hs_contacts AS contact
                            (hs_contact_id, person_key, email, firstname, lastname,
                             phone, company, lifecyclestage,
                             hs_created_at, hs_updated_at, properties)
                        VALUES
                            (:hs_contact_id, :person_key, :email, :firstname, :lastname,
                             :phone, :company, :lifecyclestage,
                             :hs_created_at, :hs_updated_at, CAST(:properties AS JSONB))
                        ON CONFLICT (hs_contact_id) DO UPDATE SET
                            person_key     = EXCLUDED.person_key,
                            email          = EXCLUDED.email,
                            firstname      = EXCLUDED.firstname,
                            lastname       = EXCLUDED.lastname,
                            phone          = EXCLUDED.phone,
                            company        = EXCLUDED.company,
                            lifecyclestage = EXCLUDED.lifecyclestage,
                            hs_created_at  = EXCLUDED.hs_created_at,
                            hs_updated_at  = EXCLUDED.hs_updated_at,
                            properties     = EXCLUDED.properties
                        WHERE (
                            contact.person_key     IS DISTINCT FROM EXCLUDED.person_key     OR
                            contact.email          IS DISTINCT FROM EXCLUDED.email          OR
                            contact.firstname      IS DISTINCT FROM EXCLUDED.firstname      OR
                            contact.lastname       IS DISTINCT FROM EXCLUDED.lastname       OR
                            contact.phone          IS DISTINCT FROM EXCLUDED.phone          OR
                            contact.company        IS DISTINCT FROM EXCLUDED.company        OR
                            contact.lifecyclestage IS DISTINCT FROM EXCLUDED.lifecyclestage OR
                            contact.hs_created_at  IS DISTINCT FROM EXCLUDED.hs_created_at  OR
                            contact.hs_updated_at  IS DISTINCT FROM EXCLUDED.hs_updated_at  OR
                            contact.properties     IS DISTINCT FROM EXCLUDED.properties
                        )
                    """),
                    {
                        "hs_contact_id":  hs_contact_id,
                        "person_key":     person_key,
                        "email":          email,
                        "firstname":      props.get("firstname"),
                        "lastname":       props.get("lastname"),
                        "phone":          props.get("phone"),
                        "company":        props.get("company"),
                        "lifecyclestage": props.get("lifecyclestage"),
                        "hs_created_at":  props.get("createdate"),
                        "hs_updated_at":  props.get("hs_lastmodifieddate"),
                        "properties":     json.dumps(props),
                    },
                )
                upserted += result.rowcount

                # Record the HubSpot ↔ person_key crosswalk for matched contacts
                if person_key is not None:
                    pg_conn.execute(
                        text(
                            "INSERT INTO person_source_identifier "
                            "  (person_key, source_system, source_entity_type, source_identifier) "
                            "VALUES (:person_key, 'hubspot', 'contact', :hs_contact_id) "
                            "ON CONFLICT DO NOTHING"
                        ),
                        {"person_key": person_key, "hs_contact_id": hs_contact_id},
                    )

        stats["row_count"] = upserted

    return {"status": "ok", "contacts_upserted": upserted}


@asset(group_name="hubspot_loaders", deps=[warehouse_schema_ready])
def hs_companies_table(
    context: dagster.AssetExecutionContext,
    hubspot_api: HubspotApiResource,
    warehouse_pg: WarehousePgResource,
):
    # TO_RESOLVE: confirm property names against live HubSpot sandbox before first run
    companies_data = list(
        hubspot_api.paginate(
            "/crm/v3/objects/companies",
            params={
                "properties": "name,domain,industry,city,state,createdate,hs_lastmodifieddate"
            },
        )
    )

    engine = warehouse_pg.get_engine()
    upserted = 0
    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="hubspot",
    ) as stats:
        with engine.begin() as pg_conn:
            for row in companies_data:
                props = row.get("properties", {})
                result = pg_conn.execute(
                    text("""
                        INSERT INTO hs_companies AS company
                            (hs_company_id, name, domain, industry, city, state,
                             hs_created_at, hs_updated_at, properties)
                        VALUES
                            (:hs_company_id, :name, :domain, :industry, :city, :state,
                             :hs_created_at, :hs_updated_at, CAST(:properties AS JSONB))
                        ON CONFLICT (hs_company_id) DO UPDATE SET
                            name          = EXCLUDED.name,
                            domain        = EXCLUDED.domain,
                            industry      = EXCLUDED.industry,
                            city          = EXCLUDED.city,
                            state         = EXCLUDED.state,
                            hs_created_at = EXCLUDED.hs_created_at,
                            hs_updated_at = EXCLUDED.hs_updated_at,
                            properties    = EXCLUDED.properties
                        WHERE (
                            company.name          IS DISTINCT FROM EXCLUDED.name          OR
                            company.domain        IS DISTINCT FROM EXCLUDED.domain        OR
                            company.industry      IS DISTINCT FROM EXCLUDED.industry      OR
                            company.city          IS DISTINCT FROM EXCLUDED.city          OR
                            company.state         IS DISTINCT FROM EXCLUDED.state         OR
                            company.hs_created_at IS DISTINCT FROM EXCLUDED.hs_created_at OR
                            company.hs_updated_at IS DISTINCT FROM EXCLUDED.hs_updated_at OR
                            company.properties    IS DISTINCT FROM EXCLUDED.properties
                        )
                    """),
                    {
                        "hs_company_id": row["id"],
                        "name":          props.get("name"),
                        "domain":        props.get("domain"),
                        "industry":      props.get("industry"),
                        "city":          props.get("city"),
                        "state":         props.get("state"),
                        "hs_created_at": props.get("createdate"),
                        "hs_updated_at": props.get("hs_lastmodifieddate"),
                        "properties":    json.dumps(props),
                    },
                )
                upserted += result.rowcount

        stats["row_count"] = upserted

    return {"status": "ok", "companies_upserted": upserted}


@asset(group_name="hubspot_loaders", deps=[hs_contacts_table, hs_companies_table])
def hs_deals_table(
    context: dagster.AssetExecutionContext,
    hubspot_api: HubspotApiResource,
    warehouse_pg: WarehousePgResource,
):
    # TO_RESOLVE: confirm property names against live HubSpot sandbox before first run
    deals_data = list(
        hubspot_api.paginate(
            "/crm/v3/objects/deals",
            params={
                "properties": (
                    "dealname,amount,pipeline,dealstage,dealtype,"
                    "closedate,createdate,hs_lastmodifieddate"
                )
            },
        )
    )

    engine = warehouse_pg.get_engine()
    upserted = 0
    with audit_load(
        engine,
        batch_id=context.run_id,
        asset_name=context.asset_key.path[-1],
        source_system="hubspot",
    ) as stats:
        with engine.begin() as pg_conn:
            for row in deals_data:
                props = row.get("properties", {})
                # HubSpot returns amount as a string — cast to NUMERIC(14,2)
                amount = _parse_amount(props.get("amount"))

                result = pg_conn.execute(
                    text("""
                        INSERT INTO hs_deals AS deal
                            (hs_deal_id, dealname, amount, pipeline, dealstage,
                             dealtype, closedate, hs_created_at, hs_updated_at, properties)
                        VALUES
                            (:hs_deal_id, :dealname, :amount, :pipeline, :dealstage,
                             :dealtype, :closedate, :hs_created_at, :hs_updated_at,
                             CAST(:properties AS JSONB))
                        ON CONFLICT (hs_deal_id) DO UPDATE SET
                            dealname      = EXCLUDED.dealname,
                            amount        = EXCLUDED.amount,
                            pipeline      = EXCLUDED.pipeline,
                            dealstage     = EXCLUDED.dealstage,
                            dealtype      = EXCLUDED.dealtype,
                            closedate     = EXCLUDED.closedate,
                            hs_created_at = EXCLUDED.hs_created_at,
                            hs_updated_at = EXCLUDED.hs_updated_at,
                            properties    = EXCLUDED.properties
                        WHERE (
                            deal.dealname      IS DISTINCT FROM EXCLUDED.dealname      OR
                            deal.amount        IS DISTINCT FROM EXCLUDED.amount        OR
                            deal.pipeline      IS DISTINCT FROM EXCLUDED.pipeline      OR
                            deal.dealstage     IS DISTINCT FROM EXCLUDED.dealstage     OR
                            deal.dealtype      IS DISTINCT FROM EXCLUDED.dealtype      OR
                            deal.closedate     IS DISTINCT FROM EXCLUDED.closedate     OR
                            deal.hs_created_at IS DISTINCT FROM EXCLUDED.hs_created_at OR
                            deal.hs_updated_at IS DISTINCT FROM EXCLUDED.hs_updated_at OR
                            deal.properties    IS DISTINCT FROM EXCLUDED.properties
                        )
                    """),
                    {
                        "hs_deal_id":    row["id"],
                        "dealname":      props.get("dealname"),
                        "amount":        amount,
                        "pipeline":      props.get("pipeline"),
                        "dealstage":     props.get("dealstage"),
                        "dealtype":      props.get("dealtype"),
                        "closedate":     props.get("closedate"),
                        "hs_created_at": props.get("createdate"),
                        "hs_updated_at": props.get("hs_lastmodifieddate"),
                        "properties":    json.dumps(props),
                    },
                )
                upserted += result.rowcount

        stats["row_count"] = upserted

    return {"status": "ok", "deals_upserted": upserted}

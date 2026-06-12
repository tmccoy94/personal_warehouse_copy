# hubspot_live_import drop folder

Drop the HubSpot **"All contacts" CRM export** CSV here (e.g.
`hubspot-crm-exports-all-contacts-live-YYYY-MM-DD.csv`), then materialize the
`hubspot_live_import_csv` asset in Dagster.

- Drop the export **exactly as HubSpot gives it to you — no cleaning needed.**
- The asset only reads these columns: `First Name, Last Name, Email, Phone Number,
  Birthdate` (`Email` is the match key). Any other columns — including
  `Number Of Products` and `Products` — are ignored, whether they're empty or
  populated. The output's product columns are built fresh from the live Keystone pull.
- Only one export should sit here at a time; the asset reads the first CSV alphabetically.
- The asset writes its result to `data/csv_output/hubspot_live_import/`.

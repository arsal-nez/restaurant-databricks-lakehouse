\# Restaurant Analytics Lakehouse Architecture



\## Source Layer



Synthetic restaurant data generated using Python.



Sources:



\- Customers

\- Restaurants

\- Menu Items

\- Historical Orders

\- Customer Reviews



\## Bronze



Raw source data is ingested into Delta tables.



\## Silver



Data is cleaned, typed and normalized.



Dimensions:



\- dim\_customer

\- dim\_restaurants

\- dim\_menu\_items



Facts:



\- fact\_orders

\- fact\_order\_items

\- fact\_reviews



Nested order item JSON is parsed and exploded into

fact\_order\_items.



\## Gold



Business-ready aggregates:



\- d\_sales\_summary

\- d\_customer\_360

\- d\_restaurant\_performance

\- d\_menu\_performance



\## Consumption



Gold tables feed the Restaurant Analytics Dashboard.



\## Orchestration



Databricks Jobs orchestrates:



Bronze

→ Silver

→ Gold

→ Data Quality


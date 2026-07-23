\# Silver Layer



The Silver layer transforms raw Bronze data into cleaned,

analytics-ready dimensional and fact tables.



\## Dimension Tables



\- `dim\_customer`

\- `dim\_restaurants`

\- `dim\_menu\_items`



\## Fact Tables



\- `fact\_orders`

\- `fact\_reviews`



\## Planned



\- `fact\_order\_items`



\## Transformations



The Silver pipeline performs:



\- Null primary-key filtering

\- Duplicate removal

\- String trimming and normalization

\- Email normalization

\- Phone number conversion to string

\- Numeric type conversion

\- Timestamp conversion

\- Invalid rating filtering

\- Referential integrity validation



\## Data Flow



Bronze customers -> Silver dim\_customer



Bronze restaurants -> Silver dim\_restaurants



Bronze menu\_items -> Silver dim\_menu\_items



Bronze historical\_orders -> Silver fact\_orders



Bronze reviews -> Silver fact\_reviews



\## Data Quality



The pipeline validates:



\- Duplicate customer IDs

\- Duplicate restaurant IDs

\- Invalid order amounts

\- Invalid timestamps

\- Invalid review ratings

\- Orders without valid customers

\- Orders without valid restaurants

\- Reviews without matching order


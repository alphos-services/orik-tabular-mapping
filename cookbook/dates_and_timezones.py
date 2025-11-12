from src.core import (
    MappingBuilder, Rule, PredicateBuilder as P,
    DeclarativeConverter
)

def main():
    record = {
        "user": {
            "id": 123, "first": "Ada", "last": "Lovelace", "score": 950,
            "emails": [
                {"type": "work", "value": "ada@company.com"},
                {"type": "personal", "value": "ada@example.com"}
            ]
        },
        "items": [
            {"name": "Widget A", "price": 3.5, "qty": 2, "category": "gear"},
            {"name": "Widget B", "price": 4.0, "qty": 0, "category": "gear"},
            {"name": "Widget C", "price": 10, "qty": 1, "category": "premium"},
        ],
        "tags": ["new", "flash-sale"],
        "created_at": "2024-07-01T12:34:56Z",
        "country_code": "DE",
        "meta": {"source": "api", "version": 2}
    }

    countries = {"DE": "Germany", "FR": "France", "US": "United States"}

    mapping = (
        MappingBuilder()
        .col("created_iso").date_parse(text=Rule().path("created_at"), formats=[], strict=False).end()
        .col("created_local").to_timezone(dt=Rule().path("created_at"), to="Europe/Berlin", from_tz="UTC").end()
        .col("created_date").date_format(parse=Rule().path("created_at"), fmt="%Y-%m-%d").end()
        .col("from_unix").from_timestamp(sec=Rule().const(1719832496), unit="s").end()
        .build()
    )

    converter = DeclarativeConverter(mapping)

    parsed_dataframe = converter.to_dataframe_single(record)
    print(parsed_dataframe.to_string(index=True))


if __name__ == "__main__":
    main()

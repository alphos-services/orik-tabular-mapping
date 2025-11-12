from src.core import (
    MappingBuilder, Rule, PredicateBuilder as P,
    DeclarativeConverter, register_udf, EngineConfig
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

    countries = {"DE": "Germany", "FR": "France"}

    mapping = (
        MappingBuilder()
        .explode("items")
        .define("fullname", Rule().concat(Rule().path("user.first"), " ", Rule().path("user.last")))
        .col("user_id").path("user.id").cast("str").end()
        .col("customer").ref("fullname").end()
        .col("email_work").path('user.emails[?type=="work"]?[0].value').end()
        .col("country").lookup(Rule().path("country_code"), countries, default="Unknown").end()
        .col("name").rel_path("name").end()
        .col("price").rel_path("price").cast("float").end()
        .col("qty").rel_path("qty").cast("int").default(0).end()
        .col("created_local").to_timezone(dt=Rule().path("created_at"), to="Europe/Berlin", from_tz="UTC").end()
        .col("is_vip").when(P.gt(Rule().path("user.score"), 900), True, False).cast("bool").end()
        .build()
    )

    cfg = EngineConfig(trace_enabled=False, default_on_error="null")

    converter = DeclarativeConverter(mapping, config=cfg)

    parsed_dataframe = converter.to_dataframe_single(record)
    print(parsed_dataframe.to_string(index=True))


if __name__ == "__main__":
    main()

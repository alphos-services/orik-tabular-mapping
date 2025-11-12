from src.core import (
    MappingBuilder, Rule, PredicateBuilder as P,
    DeclarativeConverter, register_udf
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

    full_name = Rule().concat(Rule().path("user.first"), " ", Rule().path("user.last"))

    def norm_city(name: str, country: str) -> str:
        return f"{name.strip().title()} ({country})"

    register_udf("norm_city", norm_city)

    mapping = (
        MappingBuilder()
        .col("city_norm").udf("norm_city", Rule().const(" berlin "), Rule().path("country_code")).end()
        .build()
    )

    converter = DeclarativeConverter(mapping)

    parsed_dataframe = converter.to_dataframe_single(record)
    print(parsed_dataframe.to_string(index=True))


if __name__ == "__main__":
    main()

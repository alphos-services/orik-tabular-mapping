from src.core import (
    MappingBuilder, Rule, PredicateBuilder as P,
    DeclarativeConverter, PredicateBuilder
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

    mapping = (
        MappingBuilder()
        .col("all_emails_json").map(
            over=Rule().path("user.emails"),
            apply=Rule().rel_path("value"),
            emit="json"
        ).end()

        .col("num_in_stock").filter(
            over=Rule().path("items"),
            where=PredicateBuilder.gt(Rule().rel_path("qty"), 0),
            emit="count"
        ).end()

        .col("tags_joined").sort(
            over=Rule().path("tags"),
            emit="join", sep=" | "
        ).end()
        .build()
    )

    converter = DeclarativeConverter(mapping)

    parsed_dataframe = converter.to_dataframe_single(record)
    print(parsed_dataframe.to_string(index=False))


if __name__ == "__main__":
    main()

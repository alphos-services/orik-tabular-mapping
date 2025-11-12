from src.core import (
    MappingBuilder, Rule, PredicateBuilder as P,
    DeclarativeConverter, EngineConfig,
    register_udf, register_operation
)

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
    .col("user_id").path("user.id").cast("str").end()
    .col("full_name").concat(Rule().path("user.first"), " ", Rule().path("user.last")).end()
    .build()
)

df = DeclarativeConverter(mapping).to_dataframe_single(record)
print(df)

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

    converter = DeclarativeConverter(
        MappingBuilder()
        .col("vip_flag").when(P.gt(Rule().path("user.score"), 900), True, False).end()
        .build(),
        config=EngineConfig(trace_enabled=True)
    )
    trace = converter.trace(record)
    print(trace)


if __name__ == "__main__":
    main()
